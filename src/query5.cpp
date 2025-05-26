#include "query5.hpp"
#include <iostream>
#include <fstream>
#include <sstream>
#include <thread>
#include <mutex>
#include <vector>
#include <map>
#include <string>

// Global mutex for thread-safe access to results
std::mutex results_mutex;

// Function to parse command line arguments
bool parseArgs(int argc, char* argv[], std::string& r_name, std::string& start_date, std::string& end_date, int& num_threads, std::string& table_path, std::string& result_path) {
    // Default values
    r_name = "";
    start_date = "";
    end_date = "";
    num_threads = 1; // Default to 1 thread
    table_path = "";
    result_path = "";

    // Check for minimum required arguments
    if (argc < 13) {
        std::cerr << "Insufficient arguments provided." << std::endl;
        return false;
    }

    for (int i = 1; i < argc; i++) {
        std::string arg = argv[i];

        if (arg == "--r_name" && i + 1 < argc) {
            r_name = argv[++i];
        } else if (arg == "--start_date" && i + 1 < argc) {
            start_date = argv[++i];
        } else if (arg == "--end_date" && i + 1 < argc) {
            end_date = argv[++i];
        } else if (arg == "--threads" && i + 1 < argc) {
            num_threads = std::stoi(argv[++i]);
        } else if (arg == "--table_path" && i + 1 < argc) {
            table_path = argv[++i];
        } else if (arg == "--result_path" && i + 1 < argc) {
            result_path = argv[++i];
        } else {
            std::cerr << "Unknown argument: " << arg << std::endl;
            return false;
        }
    }

    // Check if required arguments are set
    if (r_name.empty() || start_date.empty() || end_date.empty() || table_path.empty() || result_path.empty()) {
        std::cerr << "Missing required arguments." << std::endl;
        return false;
    }

    return true;
}

// Function to read TPCH data from the specified paths
bool readTPCHData(const std::string& table_path, std::vector<std::map<std::string, std::string>>& customer_data, std::vector<std::map<std::string, std::string>>& orders_data, std::vector<std::map<std::string, std::string>>& lineitem_data, std::vector<std::map<std::string, std::string>>& supplier_data, std::vector<std::map<std::string, std::string>>& nation_data, std::vector<std::map<std::string, std::string>>& region_data) {
    // Define file names for each table
    std::string customer_file = table_path + "/customer.tbl";
    std::string orders_file = table_path + "/orders.tbl";
    std::string lineitem_file = table_path + "/lineitem.tbl";
    std::string supplier_file = table_path + "/supplier.tbl";
    std::string nation_file = table_path + "/nation.tbl";
    std::string region_file = table_path + "/region.tbl";

    // Function to read a table from a file and populate the corresponding vector
    auto readTable = [](const std::string& filename, std::vector<std::map<std::string, std::string>>& data, const std::vector<std::string>& headers) {
        std::ifstream file(filename);
        if (!file.is_open()) {
            std::cerr << "Error opening file: " << filename << std::endl;
            return false;
        }
        std::string line;
        while (std::getline(file, line)) {
            std::map<std::string, std::string> row;
            std::istringstream ss(line);
            std::string value;
            size_t index = 0;
            while (std::getline(ss, value, '|')) { // Assuming '|' is the delimiter
                if (index < headers.size()) {
                    row[headers[index]] = value;
                }
                index++;
            }
            data.push_back(row);
        }
        file.close();
        return true;
    };

    // Define headers for each table
    std::vector<std::string> customer_headers = {"C_CUSTKEY", "C_NAME", "C_ADDRESS", "C_NATIONKEY", "C_PHONE", "C_ACCTBAL", "C_MKTSEGMENT", "C_COMMENT"};
    std::vector<std::string> orders_headers = {"O_ORDERKEY", "O_CUSTKEY", "O_ORDERSTATUS", "O_TOTALPRICE", "O_ORDERDATE", "O_ORDERPRIORITY", "O_CLERK", "O_SHIPPRIORITY", "O_COMMENT"};
    std::vector<std::string> lineitem_headers = {"L_ORDERKEY", "L_PARTKEY", "L_SUPPKEY", "L_LINENUMBER", "L_QUANTITY", "L_EXTENDEDPRICE", "L_DISCOUNT", "L_TAX", "L_RETURNFLAG", "L_LINESTATUS", "L_SHIPDATE", "L_COMMITDATE", "L_RECEIPTDATE", "L_SHIPINSTRUCT", "L_SHIPMODE", "L_COMMENT"};
    std::vector<std::string> supplier_headers = {"S_SUPPKEY", "S_NAME", "S_ADDRESS", "S_NATIONKEY", "S_PHONE", "S_ACCTBAL", "S_COMMENT"};
    std::vector<std::string> nation_headers = {"N_NATIONKEY", "N_NAME", "N_REGIONKEY", "N_COMMENT"};
    std::vector<std::string> region_headers = {"R_REGIONKEY", "R_NAME", "R_COMMENT"};

    // Read each table
    if (!readTable(customer_file, customer_data, customer_headers) ||
        !readTable(orders_file, orders_data, orders_headers) ||
        !readTable(lineitem_file, lineitem_data, lineitem_headers) ||
        !readTable(supplier_file, supplier_data, supplier_headers) ||
        !readTable(nation_file, nation_data, nation_headers) ||
        !readTable(region_file, region_data, region_headers)) {
        return false; // If any table fails to read, return false
    }
    return true; // Successfully read all tables
}

// Function to process a portion of the data
void processData(const std::vector<std::map<std::string, std::string>>& orders_data,
                 const std::vector<std::map<std::string, std::string>>& lineitem_data,
                 const std::string& r_name, const std::string& start_date, const std::string& end_date,
                 std::map<std::string, double>& results, size_t start, size_t end) {
    double total_revenue = 0.0;

    for (size_t i = start; i < end; ++i) {
        const auto& order = orders_data[i];
        const auto& order_date = order.at("O_ORDERDATE");

        // Check if the order date is within the specified range
        if (order_date >= start_date && order_date <= end_date) {
            for (const auto& lineitem : lineitem_data) {
                if (lineitem.at("L_ORDERKEY") == order.at("O_ORDERKEY")) {
                    double extended_price = std::stod(lineitem.at("L_EXTENDEDPRICE"));
                    double discount = std::stod(lineitem.at("L_DISCOUNT"));
                    total_revenue += extended_price * (1 - discount);
                }
            }
        }
    }

    // Lock the results map to safely update it
    std::lock_guard<std::mutex> lock(results_mutex);
    results[r_name] += total_revenue;
}

// Function to execute TPCH Query 5 using multithreading
bool executeQuery5(const std::string& r_name, const std::string& start_date, const std::string& end_date, int num_threads,
                   const std::vector<std::map<std::string, std::string>>& customer_data,
                   const std::vector<std::map<std::string, std::string>>& orders_data,
                   const std::vector<std::map<std::string, std::string>>& lineitem_data,
                   const std::vector<std::map<std::string, std::string>>& supplier_data,
                   const std::vector<std::map<std::string, std::string>>& nation_data,
                   const std::vector<std::map<std::string, std::string>>& region_data,
                   std::map<std::string, double>& results) {

    // Initialize results for the region
    results[r_name] = 0.0;

    // Determine the size of the orders data
    size_t total_orders = orders_data.size();
    size_t chunk_size = total_orders / num_threads;
    std::vector<std::thread> threads;

    // Create threads to process the data
    for (int i = 0; i < num_threads; ++i) {
        size_t start = i * chunk_size;
        size_t end = (i == num_threads - 1) ? total_orders : (start + chunk_size); // Handle the last chunk

        threads.emplace_back(processData, std::ref(orders_data), std::ref(lineitem_data), r_name, start_date, end_date, std::ref(results), start, end);
    }

    // Join threads
    for (auto& thread : threads) {
        thread.join();
    }

    return true; // Successfully executed the query
}

// Function to output results to the specified path
bool outputResults(const std::string& result_path, const std::map<std::string, double>& results) {
    std::ofstream outfile(result_path);
    if (!outfile.is_open()) {
        std::cerr << "Failed to open output file: " << result_path << std::endl;
        return false;
    }
    // Output each result as key and value separated by a tab
    for (const auto& entry : results) {
        outfile << entry.first << "\t" << entry.second << "\n";
    }
    outfile.close();
    return true;
}
