#include <SZ3/api/sz.hpp>

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

int main() {
    // std::vector<size_t> dims({100, 200, 300});
    // SZ::Config conf({dims[0], dims[1], dims[2]});

    SZ::Config conf({200000});
    conf.cmprAlgo = SZ::ALGO_INTERP_LORENZO;
    conf.errorBoundMode = SZ::EB_REL; // refer to def.hpp for all supported error bound mode
    conf.relErrorBound = 1E-4;        // rel error bound 1e-4

    const char *cmpPath = "../../../dl-retrieval/outputs/compressed_files/index-0-200000.sz";

    decompress<float>(cmpPath, conf);
    return 0;
}