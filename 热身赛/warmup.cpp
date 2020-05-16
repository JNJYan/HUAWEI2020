/*
 * @Author: JNJYan
 * @LastEditors: JNJYan
 * @Email: jjy20140825@gmail.com
 * @Date: 2020-03-27 18:25:43
 * @LastEditTime: 2020-04-27 16:01:27
 * @Description: Modify here please
 * @FilePath: /github/HUAWEI2020/warmup.cpp
 */

#include <iostream>
#include <vector>
#include <sstream>
#include <fstream>
#include <cmath>
#include <ctime>
#include <cstdlib>
#include <thread>
#include <mutex>
#include <sys/mman.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <string.h>

// #define TEST
using namespace std;

mutex mtx;
int pagesize = getpagesize();
struct Data{
    vector<float> features;
    int label;
    Data(vector<float> f, int l): features(f), label(l)
    {}
};
struct Param{
    vector<float> wtSet;
};

class LR{
public:
    void train();
    void predict();
    bool read_train();
    bool read_test();
    char nc(FILE* &f);
    LR(string trainFile, string testFile, string predictOutFile);

private:
    int featuresNum;
    float rate = 4.7;
    const float rato = 0.71;
    const int ThreadNum = 4;
    const int TestNum = 20000;
    const int TrainNum = 2000;
    const float wtInitV = 0.0;
    const int BatchSize = 500;
    const int maxIterTimes = 4;
    const float predictTrueThresh = 0.5;
    const int train_show_step = 10;

private:
    vector<vector<Data>> trainData_thread;
    vector<vector<Data>> testData_thread;
    vector<Data> trainDataSet;
    vector<Data> testDataSet;
    vector<int> predictVec;
    Param param;
    string trainFile;
    string testFile;
    string predictOutFile;
    string weightParamFile = "modelweight.txt";

private:
    bool init();
    float lossCal();
    void initParam();
    float wxbCalc(const Data &data);
    float sigmoidCalc(const float wxb);
    int storePredict(vector<int> &predict);
    void thread_read_test(int tid, int fp, int len);
    float gradientSlope(const vector<Data> &dataSet, int offset, int index, const vector<float> &sigmoidVec);
};

LR::LR(string trainF, string testF, string predictOutF)
{
    trainFile = trainF;
    testFile = testF;
    predictOutFile = predictOutF;
    featuresNum = 0;
    init();
}

void LR::initParam()
{
    for(int i = 0; i < featuresNum; ++i)
        param.wtSet.push_back(wtInitV);
}


bool LR::init()
{
    trainDataSet.clear();
    testData_thread.resize(ThreadNum);
    // clock_t loadTrainStartTime = clock();
    bool status = read_train();

    // clock_t loadTrainEndTime = clock();
    // cout << "load train data cost " << (loadTrainEndTime - loadTrainStartTime) / (float)CLOCKS_PER_SEC << "s" << endl;
    if(status != true)
        return false;
    featuresNum = trainDataSet[0].features.size();
    param.wtSet.clear();
    initParam();
    return true;
}

char LR::nc(FILE* &f) {
    static char buf [10],*p1=buf,*p2=buf;
    return p1==p2&&(p2=(p1=buf)+fread(buf,1,10,f),p1==p2)?EOF:*p1++;
}

bool LR::read_train()
{
    int i = 0;
    int x,f;
    int fp = open(trainFile.c_str(), O_RDONLY);
    int len = lseek(fp, 0, SEEK_END);
    char *mbuf = (char *)mmap(NULL, len, PROT_READ, MAP_PRIVATE, fp, 0);
    static char *p1 = mbuf;
    while(i<TrainNum)
    {
        ++i;
        vector<float> feature;
        for(char *p=p1; *p &&p-mbuf < len; ++p)
        {
            x = 0;
            f =1;
            while(*p != ',' && *p!='\n'){
                if(*p <= '9' && *p >= '0')
                    x = (x<<3) + (x<<1) + *p - 48;
                else if(*p == '-')
                    f = -1;
                ++p;
            }
            feature.push_back(x *f *0.001);
            p1 = p;
            if(*p == '\n')
                break;
        }
        ++p1;
        trainDataSet.push_back(Data(feature, (int)(feature.back() * 1000)));
        feature.pop_back();
    }
    return true;
}

void LR::thread_read_test(int tid, int fp, int len)
{
    int i = 0, j=0, x, f;
    int readline_num = TestNum / ThreadNum;
    int offset = len * tid / pagesize * pagesize;

    char* mbuf = (char *)mmap(NULL, len, PROT_READ, MAP_PRIVATE, fp, offset);
    char* p1 = mbuf;

    int bias = len * tid - offset; //mmap的offset必须是pagesize的整数倍，因此部分线程偏移指向应当读取的上一行
    p1 = p1+bias;
    // while(j++<bias)
    //     ++p1;

    while(i<readline_num)
    {
        ++i;
        vector<float> feature;
        for(char *p=p1; *p ; ++p)
        {
            x = 0;
            f =1;
            while(*p != ',' && *p!='\n')
            {
                if(*p <= '9' && *p >= '0')
                    x = (x<<3) + (x<<1) + *p - 48;
                else if(*p == '-')
                    f = -1;
                ++p;
            }
            feature.push_back(x *f *0.001);
            p1 = p;
            if(feature.size() == 1000)
                break;
        }
        int number = tid*readline_num+i-1;
        mtx.lock();
        testDataSet.push_back(Data(feature, number));
        mtx.unlock();
        ++p1;
    }
}

bool LR::read_test()
{
    int fp = open(testFile.c_str(), O_RDONLY);
    int len = lseek(fp, 0, SEEK_END);

    vector<std::thread> vec_threads;
    for(int i = 0; i < ThreadNum; ++i)
    {
        std::thread th(&LR::thread_read_test, this, i, fp, len / ThreadNum);
        vec_threads.emplace_back(std::move(th));
    }
    auto it = vec_threads.begin();
    for (; it != vec_threads.end(); ++it)
    {
        (*it).join();
    }
    return true;
}

void LR::train()
{
    float sigmoidVal;
    float wxbVal;

    for(int i = 0; i < maxIterTimes; ++i)
    {
        for(int k = 0; k < trainDataSet.size() / BatchSize; ++k)
        {
            vector<float> sigmoidVec;
            for(int j = 0; j < BatchSize; ++j)
            {
                wxbVal = wxbCalc(trainDataSet[k * BatchSize + j]);
                sigmoidVal = sigmoidCalc(wxbVal);
                sigmoidVec.push_back(sigmoidVal);
            }
            for(int j = 0; j < param.wtSet.size(); ++j)
            {
                param.wtSet[j] += rate * gradientSlope(trainDataSet, k * BatchSize, j, sigmoidVec);
            }
            rate = rate * rato;
        }
    }
}

void LR::predict()
{
    float sigVal;
    int predictVal;

    // clock_t loadTestStartTime = clock();
    read_test();
    // clock_t loadTestEndTime = clock();
    // cout << "load test data cost " << (loadTestEndTime - loadTestStartTime) / (float)CLOCKS_PER_SEC << "s" << endl;
    predictVec.resize(TestNum);
    int s = 0;
    for(auto i : testDataSet)
    {
        if(i.features.size() != 1000)
            cout << i.features.size() << "  " <<s << endl;
        s++;
    }
    for(int j = 0; j < testDataSet.size(); ++j)
    {
        sigVal = sigmoidCalc(wxbCalc(testDataSet[j]));
        predictVal = sigVal >= predictTrueThresh ? 1 : 0;
        // predictVec.push_back(predictVal);
        predictVec[testDataSet[j].label] = predictVal;
    }
    storePredict(predictVec);
}

bool loadAnswerData(string awFile, vector<int> &awVec)
{
    ifstream infile(awFile.c_str());
    if (!infile) {
        cout << "打开答案文件失败" << endl;
        exit(0);
    }

    while (infile) {
        string line;
        int aw;
        getline(infile, line);
        if (line.size() > 0) {
            stringstream sin(line);
            sin >> aw;
            awVec.push_back(aw);
        }
    }

    infile.close();
    return true;
}

int main()
{
    vector<int> answerVec;
    vector<int> predictVec;
    int correctCount;
    float accurate;
    string cur_dir = ".";
    string trainFile = cur_dir + "/data/train_data.txt";
    string testFile = cur_dir + "/data/temp_data.txt";
    string predictFile = cur_dir + "/projects/student/result.txt";
    string answerFile = cur_dir + "/projects/student/answer.txt";
    LR logist(trainFile, testFile, predictFile);
    // logist.thread_join(trainFile);
    // clock_t trainStartTime = clock();
    logist.train();
    // clock_t trainEndTime = clock();
    // cout << "Train model cost " << (trainEndTime - trainStartTime) /(float)CLOCKS_PER_SEC << "s" << endl;

    // logist.storeModel();

#ifdef TEST
    // cout << "ready to load answer data" << endl;
    loadAnswerData(answerFile, answerVec);

#endif
    // cout << "let's have a prediction test" << endl;
    // clock_t predictStartTime = clock();
    logist.predict();
    // clock_t predictEndTime = clock();
    // cout << "Predict test cost " << (predictEndTime - predictStartTime) /(float)CLOCKS_PER_SEC << "s" << endl;


#ifdef TEST
    loadAnswerData(predictFile, predictVec);
    correctCount = 0;
    for (int j = 0; j < predictVec.size(); j++) {
        if (j < answerVec.size()) {
            if (answerVec[j] == predictVec[j]) {
                correctCount++;
            }
        } else {
            cout << "answer size less than the real predicted value" << endl;
        }
    }
    
    accurate = ((float)correctCount) / answerVec.size();
    cout << "the prediction accuracy is " << accurate << endl;
#endif

    return 0;
}

int LR::storePredict(vector<int> &predict)
{
    int i = 0;
    char ch = '\n', c, cn='\0';
    int fp = open(predictOutFile.c_str(), O_CREAT|O_RDWR|O_TRUNC, 0666);
    lseek(fp, TestNum*2, SEEK_SET);
    ssize_t t = write(fp, &ch, 1);
    char *mbuf = (char *)mmap(NULL, TestNum*2, PROT_READ|PROT_WRITE, MAP_SHARED, fp, 0);
    if(mbuf == MAP_FAILED)
    {
        cout<< "mmap failed";
        exit(0);
    }
    for(int i=0; i < predict.size(); ++i)
    {
        c = char(predict[i]+48);
        memcpy(mbuf, &c, 1);
        mbuf++;
        memcpy(mbuf, &ch, 1);
        mbuf++;
    }
    munmap(mbuf, TestNum*2);
    close(fp);
    return 0;
}


float LR::wxbCalc(const Data &data)
{
    float mulSum = 0.0L;
    float wtv, feav;
    for(int i = 0; i < param.wtSet.size(); ++i)
    {
        wtv = param.wtSet[i];
        feav = data.features[i];
        mulSum += wtv * feav;
    }
    return mulSum;
}

inline float LR::sigmoidCalc(const float wxb)
{
    float expv = exp(-1 * wxb);
    float expvInv = 1 / (1 + expv);
    return expvInv;
}


float LR::lossCal()
{
    float lossV = 0.0L;
    
    for(int i = 0; i < trainDataSet.size(); ++i)
    {
        if(trainDataSet[i].label == 1)
            lossV -= trainDataSet[i].label * log(sigmoidCalc(wxbCalc(trainDataSet[i])));
        else
            lossV -= (1 - trainDataSet[i].label) * log(1- sigmoidCalc(wxbCalc(trainDataSet[i])));
    }

    lossV /= trainDataSet.size();
    return lossV;
}

float LR::gradientSlope(const vector<Data> &dataSet, int offset, int index, const vector<float> &sigmoidVec)
{
    float gsV = 0.0L;
    float sigv, label;
    
    for(int i = 0; i < BatchSize; ++i)
    {
        sigv = sigmoidVec[i];
        label = dataSet[offset + i].label;
        gsV += (label - sigv) * (dataSet[offset + i].features[index]);
    }
    gsV = gsV / BatchSize;
    return gsV;
}