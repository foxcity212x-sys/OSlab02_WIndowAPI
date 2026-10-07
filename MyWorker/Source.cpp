#include <iostream>
#include <fstream>
#include <string>
#include <vector>
#include <sstream>
#define NOMINMAX
#include <windows.h>
#include <map>
#include <algorithm>
#include <limits>


struct SensorRecord {
    std::string sensor_id;
    std::string timestamp;
    double value;
};

struct NormalizedRecord {
    double normalized;
    std::string status;
};

// Структура, яку ми будемо передавати кожному потоку
struct ThreadContext {
    int thread_index;
    size_t begin_idx;
    size_t end_idx;
    const std::vector<SensorRecord>* inputData;
    std::vector<NormalizedRecord>* outputData;
    double min_val;
    double max_val;
    size_t processed_count; // Власний лічильник потоку
};

// Робоча функція потоку (відповідає вимогам Windows API)
DWORD WINAPI WorkerProc(LPVOID lpParam) {
    ThreadContext* ctx = (ThreadContext*)lpParam;
    ctx->processed_count = 0;

    for (size_t i = ctx->begin_idx; i < ctx->end_idx; ++i) {
        double val = (*ctx->inputData)[i].value;

        // Обчислення нормалізованого значення
        double normalized = (val - ctx->min_val) / (ctx->max_val - ctx->min_val);
        (*ctx->outputData)[i].normalized = normalized;

        // Визначення статусу
        if (val < ctx->min_val || val > ctx->max_val) {
            (*ctx->outputData)[i].status = "OUT";
        }
        else {
            (*ctx->outputData)[i].status = "OK";
        }
        ctx->processed_count++;
    }
    return 0; // Код завершення потоку
}

std::vector<std::string> SplitString(const std::string& str, char delimiter) {
    std::vector<std::string> tokens;
    std::string token;
    std::istringstream tokenStream(str);
    while (std::getline(tokenStream, token, delimiter)) {
        tokens.push_back(token);
    }
    return tokens;
}

int main(int argc, char* argv[]) {
    int K = 0;
    double min_val = 0.0;
    double max_val = 0.0;
    bool has_min = false, has_max = false;

    // 1. Читаємо аргументи
    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "--min" && i + 1 < argc) {
            min_val = std::stod(argv[++i]);
            has_min = true;
        }
        else if (arg == "--max" && i + 1 < argc) {
            max_val = std::stod(argv[++i]);
            has_max = true;
        }
        else {
            try { K = std::stoi(arg); }
            catch (...) {}
        }
    }

    if (K <= 0 || !has_min || !has_max || max_val <= min_val) {
        std::cerr << "Помилка: Некоректні аргументи! Очікується: K --min X --max Y (max > min)" << std::endl;
        return 1;
    }

    std::cout << "Запуск MyWorker: потоків K=" << K << ", min=" << min_val << ", max=" << max_val << std::endl;

    // 2. Читання CSV
    std::vector<SensorRecord> inputData;
    std::ifstream inFile("sensors.csv");
    if (!inFile.is_open()) return 1;

    std::string line;
    std::getline(inFile, line);

    int lineNumber = 2;
    while (std::getline(inFile, line)) {
        if (line.empty()) continue;
        std::vector<std::string> tokens = SplitString(line, ';');
        if (tokens.size() != 3) {
            std::cerr << "Помилка: хибна кількість полів у рядку " << lineNumber << std::endl;
            return 1;
        }
        try {
            SensorRecord record;
            record.sensor_id = tokens[0];
            record.timestamp = tokens[1];
            record.value = std::stod(tokens[2]);
            inputData.push_back(record);
        }
        catch (...) { return 1; }
        lineNumber++;
    }
    inFile.close();

    size_t total_records = inputData.size();
    std::vector<NormalizedRecord> outputData(total_records);

    // 3. Підготовка даних для потоків
    std::vector<ThreadContext> contexts(K);
    std::vector<HANDLE> hThreads(K);
    std::vector<DWORD> tids(K);

    size_t chunk_size = total_records / K;
    size_t remainder = total_records % K;
    size_t current_idx = 0;

    for (int i = 0; i < K; ++i) {
        contexts[i].thread_index = i;
        contexts[i].begin_idx = current_idx;
        size_t current_chunk = chunk_size + (i < remainder ? 1 : 0);
        contexts[i].end_idx = current_idx + current_chunk;

        contexts[i].inputData = &inputData;
        contexts[i].outputData = &outputData;
        contexts[i].min_val = min_val;
        contexts[i].max_val = max_val;
        current_idx += current_chunk;

        // Створюємо потік у призупиненому стані
        hThreads[i] = CreateThread(NULL, 0, WorkerProc, &contexts[i], CREATE_SUSPENDED, &tids[i]);
        if (hThreads[i] == NULL) {
            std::cerr << "Помилка створення потоку " << i << std::endl;
            return 1;
        }
    }

    // Відновлюємо (запускаємо) всі потоки
    std::cout << "Запуск робочих потоків..." << std::endl;
    for (int i = 0; i < K; ++i) {
        ResumeThread(hThreads[i]);
    }

    // Головний потік чекає на завершення всіх робочих потоків
    WaitForMultipleObjects(K, hThreads.data(), TRUE, INFINITE);

    // Закриваємо дескриптори потоків
    for (int i = 0; i < K; ++i) {
        CloseHandle(hThreads[i]);
    }
    std::cout << "Усі потоки успішно завершили роботу." << std::endl;

    // 4. Запис normalized.csv
    std::ofstream outNorm("normalized.csv");
    outNorm << "sensor_id;timestamp;value;normalized;status\n";
    for (size_t i = 0; i < total_records; ++i) {
        outNorm << inputData[i].sensor_id << ";"
            << inputData[i].timestamp << ";"
            << inputData[i].value << ";"
            << outputData[i].normalized << ";"
            << outputData[i].status << "\n";
    }
    outNorm.close();

    // 5. Групування для stats.csv
    struct SensorStats {
        int count = 0;
        double min_val = std::numeric_limits<double>::max();
        double max_val = std::numeric_limits<double>::lowest();
        double sum = 0;
        int out_count = 0;
    };

    std::map<std::string, SensorStats> stats;
    for (size_t i = 0; i < total_records; ++i) {
        auto& s = stats[inputData[i].sensor_id];
        s.count++;
        s.sum += inputData[i].value;
        if (inputData[i].value < s.min_val) s.min_val = inputData[i].value;
        if (inputData[i].value > s.max_val) s.max_val = inputData[i].value;
        if (outputData[i].status == "OUT") s.out_count++;
    }

    std::ofstream outStats("stats.csv");
    outStats << "sensor_id;count;min;max;mean;out_count\n";
    for (const auto& pair : stats) {
        double mean = pair.second.sum / pair.second.count;
        outStats << pair.first << ";"
            << pair.second.count << ";"
            << pair.second.min_val << ";"
            << pair.second.max_val << ";"
            << mean << ";"
            << pair.second.out_count << "\n";
    }
    outStats.close();

    std::cout << "Файли normalized.csv та stats.csv успішно збережено." << std::endl;

    return 0;
}