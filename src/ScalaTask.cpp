//
// Created by erosh on 1/6/24.
//

#include <seastar/core/thread.hh>
#include <seastar/core/future.hh>
#include <seastar/core/sstring.hh>
#include <seastar/http/httpd.hh>
#include <seastar/http/function_handlers.hh>
#include <seastar/core/condition-variable.hh>
#include <seastar/http/json_path.hh>
#include "json.hpp"
#include <list>

#include <seastar/coroutine/all.hh>

using namespace seastar;
using namespace seastar::httpd;
using namespace std::chrono_literals;
using json = nlohmann::json;

static constexpr int MAX_KEY_LENGTH = 256;
static constexpr std::string KEY_IS_TOO_LONG = "Key is too long";

class server_shutdown {
public:
    void shutdown() {
        _shutdown_requested = true;
        _shutting_down.signal();
    }

    future<> await_shutdown() {
        return _shutting_down.wait([this] { return _shutdown_requested; });
    }

private:
    bool _shutdown_requested = false;
    seastar::condition_variable _shutting_down;
};

// Global object controls server shutdown
server_shutdown shutdown_control;

class SimpleDB {
public:
    SimpleDB(const nlohmann::json &config) {
        // Initialize cache size and database path from JSON
        cache_size_ = config["cache_size"].get<uint64_t>();
        db_path_ = config["db_path"].get<std::string>();

        // Load data from the database file
        load_from_disk();
    }

    ~SimpleDB() = default;

//    void insert_or_update(const std::string &key, const std::string &value) {
//        std::lock_guard lock(mutex_);
//        // Insert or update key/value pair in the unordered_map and update the key "priority"
//        if (const auto it = keyToData_.find(key); it != keyToData_.end()) {
//            data_.splice(data_.end(), data_, it->second);
//        } else {
//            check_cache_size();
//            data_.push_back({key, value});
//        }
//        keyToData_[key] = std::prev(data_.end());
//        db_[key] = value;
//
//        // Save data to disk (not called on every operation, but for example, on a timer or when needed)
//        save_to_disk();
//    }

    future<void> insert_or_update(std::string &&key, std::string &&value) {
        return with_semaphore(sem_, 1, [this, key = std::move(key), value = std::move(value)] {
            // Insert or update key/value pair in the unordered_map and update the key "priority"
            if (const auto it = keyToData_.find(key); it != keyToData_.end()) {
                data_.splice(data_.end(), data_, it->second);
            } else {
                check_cache_size();
                data_.push_back({key, value});
            }
            keyToData_[key] = std::prev(data_.end());
            db_[key] = value;

            // Save data to disk (not called on every operation, but for example, on a timer or when needed)
            save_to_disk();
        });
    }

//    std::string query(const std::string &key) {
//        std::lock_guard lock(mutex_);
//        // Query value by key
//        if (const auto it = keyToData_.find(key); it != keyToData_.end()) {
//            data_.splice(data_.end(), data_, it->second);
//            keyToData_[key] = std::prev(data_.end());
//            return it->second->second;
//        }
//
//        if (const auto it = db_.find(key); it != db_.end()) {
//            check_cache_size();
//            std::string value = it->get<std::string>();
//            data_.push_back({key, value});
//            keyToData_[key] = std::prev(data_.end());
//            return value;
//        }
//        return "";
//    }

    future<std::string> query(std::string &&key) {
        return with_semaphore(sem_, 1, [this, key = std::move(key)] {
            // Query value by key
            if (const auto it = keyToData_.find(key); it != keyToData_.end()) {
                data_.splice(data_.end(), data_, it->second);
                keyToData_[key] = std::prev(data_.end());
                return it->second->second;
            }

            if (const auto it = db_.find(key); it != db_.end()) {
                check_cache_size();
                std::string value = it->get<std::string>();
                data_.push_back({key, value});
                keyToData_[key] = std::prev(data_.end());
                return value;
            }
            return std::string{};
        });
    }

    future<std::vector<std::string>> query_all_keys() const {
        return with_semaphore(sem_, 1, [this] {
            std::vector<std::string> keys;
            keys.reserve(db_.size());
            for (const auto &entry: db_.items()) {
                keys.push_back(entry.key());
            }
            std::sort(keys.begin(), keys.end());
            return keys;
        });
    }

//    std::vector<std::string> query_all_keys() const {
//        // Get sorted list of all keys
//        std::lock_guard lock(mutex_);
//        std::vector<std::string> keys;
//        keys.reserve(db_.size());
//        for (const auto &entry: db_.items()) {
//            keys.push_back(entry.key());
//        }
//        std::sort(keys.begin(), keys.end());
//        return keys;
//    }

//    size_t remove(const std::string &key) {
//        // Remove key and its value
//        std::lock_guard lock(mutex_);
//        if (const auto it = keyToData_.find(key); it != keyToData_.end()) {
//            data_.erase(it->second);
//            keyToData_.erase(it);
//        }
//
//        const auto count = db_.erase(key);
//        // Save changed data to disk
//        save_to_disk();
//        return count;
//    }

    future<size_t> remove(
            std::string &&key) { // Async operation to remove the key and save the changes to disk within a locked section
        return with_semaphore(sem_, 1, [this, key = std::move(key)] {
            // Remove the key and get a count of erased elements
            if (const auto it = keyToData_.find(key); it != keyToData_.end()) {
                data_.erase(it->second);
                keyToData_.erase(it);
            }
            size_t count = db_.erase(key);
            save_to_disk();
            // Return the number of erased elements to the caller
            return count;
        });
    }

    void check_cache_size() {
        while (data_.size() >= cache_size_) {
            keyToData_.erase(data_.front().first);
            data_.pop_front();
        }
    }

    future<> flush_and_stop() {
        // Async operation to flush data to disk and then release the semaphore
        return with_semaphore(sem_, 1, [this] {
            return seastar::async([this] {
                save_to_disk();
            });
        });
    }

private:
    using DataList = std::list<std::pair<std::string, std::string>>;

    uint64_t cache_size_;
    std::filesystem::path db_path_;
    nlohmann::json db_;
    DataList data_;
    std::unordered_map<std::string, DataList::iterator> keyToData_;
    //mutable std::mutex mutex_;
    mutable seastar::semaphore sem_{1};

    void load_from_disk() {
        try {
            // Read JSON data from file
            std::ifstream file(db_path_);
            if (file.is_open()) {
                db_ = nlohmann::json::parse(file);
                file.close();
            }
        } catch (const std::exception &e) {
            std::cerr << "Error loading data from disk: " << e.what() << std::endl;
        }
    }

    void save_to_disk() {
        try {
            // Save JSON data to file
            std::ofstream file(db_path_);
            if (file.is_open()) {
                file << std::setw(4) << db_ << std::endl;
                file.close();
            }
        } catch (const std::exception &e) {
            std::cerr << "Error saving data to disk: " << e.what() << std::endl;
        }
    }
};

future<std::unique_ptr<http::reply>>
handle_insert_update(std::unique_ptr<http::request> req, std::unique_ptr<http::reply> rep, SimpleDB &db) {
    // Handle insert or update of key/value pair
    const auto key = req->get_query_param("key");
    const auto value = req->get_query_param("value");

    if (!key.empty() && !value.empty() && key.size() <= MAX_KEY_LENGTH) {
        // Insert or update value in the database
//        db.insert_or_update(key, value);
//        rep->_content = std::format("Inserted key({}), value({})", std::string(key), std::string(value));
//        rep->done("html");
//        return make_ready_future<std::unique_ptr<http::reply>>(std::move(rep));

        rep->_content = std::format("Inserted key({}), value({})", std::string(key), std::string(value));
        rep->done("html");
        return db.insert_or_update(std::move(key), std::move(value)).then([rep = std::move(rep)]() mutable {
            return std::move(rep);
        });
    } else {
        // If any of the parameters is missing, return an error to the client
        rep->_status = httpd::reply::status_type::bad_request;
        rep->_content = key.size() > MAX_KEY_LENGTH ? KEY_IS_TOO_LONG : "Missing 'key' or 'value' query parameter";
        rep->done("html");
        return make_ready_future<std::unique_ptr<http::reply>>(std::move(rep));
    }
}

future<std::unique_ptr<http::reply>>
handle_query_key(std::unique_ptr<http::request> req, std::unique_ptr<http::reply> rep, SimpleDB &db) {
    // Handle query for value by key
    const auto key = req->get_query_param("key");
    if (!key.empty() && key.size() <= MAX_KEY_LENGTH) {
//        rep->_content = db.query(key);
//        rep->done("html");
//        return make_ready_future<std::unique_ptr<http::reply>>(std::move(rep));
        return db.query(std::move(key)).then([rep = std::move(rep)](std::string &&value) mutable {
            rep->_content = std::move(value); // Maybe copy elision would work here and std::move is not required
            rep->done("html");
            return std::move(rep);
        });
    } else {
        // If key parameter is missing, return an error to the client
        rep->_status = httpd::reply::status_type::bad_request;
        rep->_content = key.size() > MAX_KEY_LENGTH ? KEY_IS_TOO_LONG : "Missing 'key' query parameter";
        rep->done("html");
        return make_ready_future<std::unique_ptr<http::reply>>(std::move(rep));
    }
}

future<std::unique_ptr<http::reply>>
handle_query_all_keys(std::unique_ptr<http::request> req, std::unique_ptr<http::reply> rep, SimpleDB &db) {
    // Handle query for the list of all keys

//    const auto keys = db.query_all_keys();
//    std::string result{};
//    for (const auto key: keys) {
//        result.append(std::format("{}, ", key));
//    }
//    if (!result.empty()) {
//        result.resize(result.size() - 2);
//    }
//    rep->_content = result;
//    rep->done("html");
//    return make_ready_future<std::unique_ptr<http::reply>>(std::move(rep));

    return db.query_all_keys().then([rep = std::move(rep)](std::vector<std::string> keys) mutable {
        std::string result{};
        for (const auto key: keys) {
            result.append(std::format("{}, ", key));
        }
        if (!result.empty()) {
            result.resize(result.size() - 2);
        }
        rep->_content = result;
        rep->done("html");
        return std::move(rep);
    });
}

future<std::unique_ptr<http::reply>>
handle_delete_key(std::unique_ptr<http::request> req, std::unique_ptr<http::reply> rep, SimpleDB &db) {
    // Handle deletion of key
    const auto key = req->get_query_param("key");
    if (!key.empty() && key.size() <= MAX_KEY_LENGTH) {
//        rep->_content = std::format("Deleted {} elements", db.remove(key));
//        rep->done("html");
//        return make_ready_future<std::unique_ptr<http::reply>>(std::move(rep));
        return db.remove(std::move(key)).then([rep = std::move(rep)](size_t count) mutable {
            rep->_content = std::format("Deleted {} elements", count);
            rep->done("html");
            return std::move(rep);
        });
    } else {
        // If key parameter is missing, return an error to the client
        rep->_status = httpd::reply::status_type::bad_request;
        rep->_content = key.size() > MAX_KEY_LENGTH ? KEY_IS_TOO_LONG : "Missing 'key' query parameter";
        rep->done("html");
        return make_ready_future<std::unique_ptr<http::reply>>(std::move(rep));
    }
}

void set_routes(routes &r, SimpleDB &db) {
    r.add(operation_type::POST, url("/insert_update"), new function_handler(
            [&db](std::unique_ptr<http::request> req, std::unique_ptr<http::reply> rep) {
                return handle_insert_update(std::move(req), std::move(rep), db);
            }, "html"
    ));
    r.add(operation_type::GET, url("/insert_update"), new function_handler(
            [&db](std::unique_ptr<http::request> req, std::unique_ptr<http::reply> rep) {
                return handle_insert_update(std::move(req), std::move(rep), db);
            }, "html"
    ));
    r.add(operation_type::GET, url("/query_key"), new function_handler(
            [&db](std::unique_ptr<http::request> req, std::unique_ptr<http::reply> rep) {
                return handle_query_key(std::move(req), std::move(rep), db);
            }, "html"
    ));
    r.add(operation_type::GET, url("/query_all_keys"), new function_handler(
            [&db](std::unique_ptr<http::request> req, std::unique_ptr<http::reply> rep) {
                return handle_query_all_keys(std::move(req), std::move(rep), db);
            }, "html"
    ));
    r.add(operation_type::DELETE, url("/delete_key"), new function_handler(
            [&db](std::unique_ptr<http::request> req, std::unique_ptr<http::reply> rep) {
                return handle_delete_key(std::move(req), std::move(rep), db);
            }, "html"
    ));
    r.add(operation_type::GET, url("/delete_key"), new function_handler(
            [&db](std::unique_ptr<http::request> req, std::unique_ptr<http::reply> rep) {
                return handle_delete_key(std::move(req), std::move(rep), db);
            }, "html"
    ));
    r.add(operation_type::PUT, url("/shutdown"), new function_handler([](std::unique_ptr<http::request> req) {
        shutdown_control.shutdown();
        return make_ready_future<seastar::json::json_return_type>("OK");
    }));
    r.add(operation_type::GET, url("/shutdown"), new function_handler([](std::unique_ptr<http::request> req) {
        shutdown_control.shutdown();
        return make_ready_future<seastar::json::json_return_type>("OK");
    }));
}

int main(int argc, char **argv) {
    nlohmann::json default_config = R"({
        "cache_size": 5,
        "db_path": "database.json",
        "ip" : "127.0.0.1",
        "port" : 8080
    })"_json;

    static const char options_title[] = "SimpleDB http server";

    boost::program_options::options_description cli_options(options_title);
    boost::program_options::variables_map options;

    try {
        cli_options.add_options()
                ("help,h", "Display help message")
                ("config,c", boost::program_options::value<std::filesystem::path>(), ("Path to config json file."));

        boost::program_options::positional_options_description pos_desc;
        pos_desc.add("input", -1);

        boost::program_options::command_line_parser parser{argc, argv};
        parser.options(cli_options).positional(pos_desc).allow_unregistered();

        boost::program_options::store(parser.run(), options);
        boost::program_options::notify(options);
    } catch (const boost::program_options::error &e) {
        std::cout << "Error parsing command line: " << e.what() << std::endl;
        return -1;
    }

    if (options.count("help")) {
        cli_options.print(std::cout);
        return 0;
    }

    try {
        if (!options.count("config")) {
            std::cout << std::format("You haven't specify the path to json config file. Using default:\n{}",
                                     default_config.dump(1)) << std::endl;
        } else {
            std::ifstream ifstream(options["config"].as<std::filesystem::path>());
            default_config = nlohmann::json::parse(ifstream);
        }
    } catch (const std::exception &e) {
        throw std::logic_error("Failed to parse config file " +
                               std::string(options["input"].as<std::filesystem::path>()) + ": " + e.what());
    }

    SimpleDB db(default_config);
    auto server = std::make_unique<http_server_control>();
    app_template app;
    app.add_options()
            ("server-addr", boost::program_options::value<std::string>()->default_value(
                     std::format("{}:{}", default_config.at("ip").get<std::string>(),
                                 default_config.at("port").get<int>())),
             "IP address and port to listen on");

    return app.run(argc, argv, [&app, &server, &db] -> future<int> {
        auto &config = app.configuration();
        std::string server_addr_str = config["server-addr"].as<std::string>();
        ipv4_addr addr{server_addr_str};
        co_await server->start().then([&server, &db] {
            return server->set_routes([&db](routes &r) {
                set_routes(r, db);
            });
        }).then([&server, addr] {
            return server->listen(addr);
        }).then([addr] {
            std::cout << "Seastar HTTP server listening on port " << addr.port << " ...\n";
        }).handle_exception([](std::exception_ptr e) {
            std::cout << "Failed to start HTTP server: " << e << "\n";
        });
        co_await shutdown_control.await_shutdown();
        std::cout << "Seastar HTTP server shutting down...\n";
        co_await server->stop();
        co_await db.flush_and_stop();
        co_return 0;
    });
}
