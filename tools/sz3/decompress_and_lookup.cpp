#include <SZ3/api/sz.hpp>
#include <vector>

template <class T>
void compress(char *inPath, char *cmpPath, SZ::Config conf) {
    T *data = new T[conf.num];
    SZ::readfile<T>(inPath, conf.num, data);

    size_t outSize;
    SZ::Timer timer(true);
    char *bytes = SZ_compress<T>(conf, data, outSize);
    double compress_time = timer.stop();

    char outputFilePath[1024];
    if (cmpPath == nullptr) {
        snprintf(outputFilePath, 1024, "%s.sz", inPath);
    } else {
        strcpy(outputFilePath, cmpPath);
    }
    SZ::writefile(outputFilePath, bytes, outSize);

    printf("compression ratio = %.2f \n", conf.num * 1.0 * sizeof(T) / outSize);
    printf("compression time = %f\n", compress_time);
    printf("compressed data file = %s\n", outputFilePath);

    delete[] data;
    delete[] bytes;
}

template <class T>
T *decompress(const char *cmpPath, SZ::Config conf) {
    size_t cmpSize;
    auto cmpData = SZ::readfile<char>(cmpPath, cmpSize);

    SZ::Timer timer(true);
    T *decData = SZ_decompress<T>(conf, cmpData.get(), cmpSize);
    double compress_time = timer.stop();

    printf("compression ratio = %f\n", conf.num * sizeof(T) * 1.0 / cmpSize);
    printf("decompression time = %f seconds.\n", compress_time);
    return decData;
}

std::pair<std::vector<char *>, std::vector<std::vector<int>>> get_partitions_and_ids(std::vector<int> &all_ids) {
    std::vector<char *> partitionFileNames = {"partition1", "partition2", "partition3"};
    std::vector<std::vector<int>> partitionIds = {{1, 2, 3}, {4, 5, 6}, {7, 8, 9}};
    return std::make_pair(partitionFileNames, partitionIds);
}

float *retrieve(const char *cmpPath, std::vector<int> &ids, SZ::Config &conf) {
    // Get all partion file names

    // For each partition file, decompress it and look up the data

    float *decData = decompress<float>(cmpPath, conf);

    return decData;
}

template <class T>
size_t count_elements_in_vector_of_vector(std::vector<std::vector<T>> &v) {
    size_t count = 0;
    for (auto &sub : v) {
        count += sub.size();
    }
    return count;
}

int main() {
    // Start timer
    SZ::Timer timer(true);

    // Get configurations
    std::vector<size_t> dims({100, 200, 300});
    SZ::Config conf({dims[0], dims[1], dims[2]});
    conf.cmprAlgo = SZ::ALGO_INTERP_LORENZO;
    conf.errorBoundMode = SZ::EB_REL; // refer to def.hpp for all supported error bound mode
    conf.absErrorBound = 1E-3;        // absolute error bound 1e-3

    // Column names
    const std::string colNames[5] = {"dst", "enumber", "etime", "rnumber"};

    // All ids to query
    std::vector<int> all_ids({1, 2, 3, 4, 5, 6, 7, 8, 9});
    // Get partition file names and ids
    auto [partitions, partitionIds] = get_partitions_and_ids(all_ids);

    // For each partition file, decompress it and look up the data, then merge the results
    size_t n_ele = count_elements_in_vector_of_vector(partitionIds);
    std::vector<float> results;
    results.reserve(n_ele);
    for (int i = 0; i < partitions.size(); i++) {
        // Get ids to query for this partition
        std::vector<int> ids = partitionIds[i];
        // Decompress and look up data
        for (auto colName : colNames) {
            std::string partionName = colName + "_" + partitions[i];
            float *decData = retrieve(partionName.c_str(), ids, conf);
            // Merge float array decData to std::vector<float> results
            results.insert(results.end(), decData, decData + ids.size());
            // Free decData
            delete[] decData;
        }
    }
    // Stop timer and measure overall retrieval time
    double compress_time = timer.stop();
    printf("Overall retrieval time = %f seconds.\n", compress_time);
    return 0;
}