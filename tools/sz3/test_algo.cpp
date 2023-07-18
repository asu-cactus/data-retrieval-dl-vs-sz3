#include <iostream>
#include <vector>

void print_vector(std::vector<std::pair<int, int>> &v) {
    std::cout << "Vector:\n";
    for (auto ele : v) {
        std::cout << ele.first << "," << ele.second << "\n";
    }
}

int main() {
    std::vector<int> v1({0, 1, 1, 1, 1, 2, 2, 2, 3, 3, 3, 3, 4});
    std::vector<std::pair<int, int>> end_indices;

    for (auto itr = v1.begin(); itr != v1.end(); ++itr) {
        if (*itr != *(itr + 1)) {
            end_indices.push_back(std::make_pair(itr - v1.begin(), *itr));
        }
    }
    print_vector(end_indices);
    return 0;
}