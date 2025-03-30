#ifndef MOCK_REPOSITORY_HPP
#define MOCK_REPOSITORY_HPP

#include "repository.hpp"

class MockRepository : public Repository {
public:
    DataFrame load() override {
        DataFrame df;
        df.addRow({1, "Paciente A", 98.6});
        df.addRow({2, "Paciente B", 99.1});
        return df;
    }
};

#endif
