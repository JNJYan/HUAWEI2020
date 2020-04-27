/*
 * @Author: JNJYan
 * @LastEditors: JNJYan
 * @Email: jjy20140825@gmail.com
 * @Date: 2020-04-13 10:32:13
 * @LastEditTime: 2020-04-27 16:01:19
 * @Description: Modify here please
 * @FilePath: /github/HUAWEI2020/0428_4.cpp
 */
#include <bits/stdc++.h>
#include <unistd.h>
#include <fcntl.h>
#include <string>
#include <thread>
#include <pthread.h>
#include <mutex>
#include <chrono>
#include <algorithm>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
using namespace std;
using namespace chrono;

#define DEPTH_HIGH 7
#define DEPTH_LOW 3
#define NODES 50002
#define ThreadNum 4 
// #define TEST

string curDir = "";
// string testFile = curDir + "/data/351244.txt";
string testFile = curDir + "/data/test_data.txt";
string outputFile = curDir + "/projects/student/result.txt";
mutex mtx;

struct Path{
    int len;
    int path[7];
    Path(){};
    Path(int length, int *p){
        len = length;
        memcpy(path, p, len*sizeof(int));
    };
};

struct Next1s{
    int next_num;
    int prev_num;
    int nexts[50];
    int prevs[250];
};

struct Next1s Graph[NODES];
char idmid[NODES][7];
int idlen[NODES];
char (*idmidp)[7] = idmid;
int inputs[560000];
int *t = inputs;

struct Path ans[5][ThreadNum][1500000];

int ansnum[5][ThreadNum];
int nodeNum = 50002;
int ansNum = 0;

bool isNode[280000];

void readData(string &TestFile){
    int fp = open(TestFile.c_str(), O_RDONLY);
    int len = lseek(fp, 0, SEEK_END);
    char* mbuf = (char *)mmap(NULL, len, PROT_READ, MAP_PRIVATE, fp, 0);
    if(-1==madvise(mbuf, len, MADV_WILLNEED | MADV_SEQUENTIAL))
    {
        cout << "madvise error"  << endl;
    }
    char* p = mbuf;
    int x;
    int cnt = 0;
    int v, w;
    while(p-mbuf < len)
    {
        for(int i=0; i<3; ++i){
            x = 0;
            while(i<2 && *p != ','){
                if(*p <= '9' && *p >= '0')
                    x = (x<<3) + (x<<1) + *p - 48;
                ++p;
            }
            if(i<2){
                *t++ = x;
                isNode[x] = true;
                ++p;
            }
            else
                while(*p++ != '\n');
        }
    }
    close(fp);
}

void buildGraph(){
    for(int i=0; i<50001; ++i)
    {
        if(isNode[i])
        {
            sprintf(idmidp[i], "%d,", i);
            if(i>9999)
                idlen[i] = 6;
            else if(i>999)
                idlen[i] = 5;
            else if(i>99)
                idlen[i] = 4;
            else if(i>9)
                idlen[i] = 3;
            else
                idlen[i] = 2;
        }
    }
    // auto endTime = system_clock::now();
    // auto duration = duration_cast<microseconds>(endTime - startTime);
    // cout <<  "convert id to str cost: " << double(duration.count()) * microseconds::period::num / microseconds::period::den << " s." << endl;
    int v, w;
    for(auto p=inputs; p!=t;)
    {
        v = *p++;
        w = *p++;
        if(v >= 50001 || w >= 50001)
            continue;
        Graph[v].nexts[Graph[v].next_num++] = w;
        Graph[w].prevs[Graph[w].prev_num++] = v;
    }
}

void DFS2(int head, int tid, int* path, bool* onStack, bool* reach1, bool* reach2, int (*jump2)[50], int* jump2_num, vector<vector<int>>* jump3){
    int *next1, *next2, *next3, *next4, *next5, *next6;
    path[0] = head;
    onStack[head] = true;
    for(next1=Graph[head].nexts+Graph[head].next_num-1; next1>=Graph[head].nexts; --next1)
    {
        if(*next1 < head)
            break;
        path[1] = *next1;
        onStack[*next1] = true;
        for(next2=Graph[*next1].nexts+Graph[*next1].next_num-1; next2>=Graph[*next1].nexts; --next2)
        {
            if(*next2 < head)
                break;
            else if(onStack[*next2])
                continue;
            path[2] = *next2;
            onStack[*next2] = true;
            for(next3=Graph[*next2].nexts+Graph[*next2].next_num-1; next3>=Graph[*next2].nexts; --next3)
            {
                if(*next3 < head)
                    break;
                else if((*next3 !=head && onStack[*next3]))
                    continue;
                else if(*next3 == head){
                    ans[0][tid][ansnum[0][tid]++] = Path(3, path);
                    continue;
                }
                path[3] = *next3;
                onStack[*next3] = true;
                for(next4=Graph[*next3].nexts+Graph[*next3].next_num-1; next4>=Graph[*next3].nexts; --next4)
                {
                    if(*next4 < head)
                        break;
                    else if (*next4 !=head && onStack[*next4])
                        continue;
                    else if(*next4 == head){
                        ans[1][tid][ansnum[1][tid]++] = Path(4, path);
                        continue;
                    }
                    path[4] = *next4;
                    onStack[*next4] = true;
                    for(next5=Graph[*next4].nexts+Graph[*next4].next_num-1; next5>=Graph[*next4].nexts; --next5)
                    {
                        if(*next5 < head)
                            break;
                        else if(*next5 != head && onStack[*next5])
                            continue;
                        else if(*next5 == head){
                            ans[2][tid][ansnum[2][tid]++] = Path(5, path);
                            continue;
                        }
                    }
                    if(reach1[*next4])
                    {
                        for(int i=0; i<jump2_num[*next4]; ++i){
                            int node = jump2[*next4][i];
                            if(onStack[node])
                                continue;
                            path[5] = node;
                            ans[3][tid][ansnum[3][tid]++] = Path(6, path);
                        }
                    }
                    if(reach2[*next4])
                    {
                        for(auto &node: jump3[*next4]){
                            if(onStack[node[0]] || onStack[node[1]])
                                continue;
                            path[5] = node[0];
                            path[6] = node[1];
                            ans[4][tid][ansnum[4][tid]++] = Path(7, path);
                        }    
                    }
                    onStack[*next4] = false;
                }
                onStack[*next3] = false;
            }
            onStack[*next2] = false;
        }
        onStack[*next1] = false;
    }
    onStack[head] = false;
}

void single_findCycle(int tid){
    int *temp1 = new int[nodeNum];
    int tmp1 = 0;
    int *temp2 = new int[nodeNum];
    int tmp2 = 0;
    int (*jump2)[50] = new int[NODES][50];
    // int jump2_t[NODES][50];
    int jump2_num[NODES];
    memset(jump2_num, 0, nodeNum*sizeof(int));
    vector<vector<int>>* jump3 = new vector<vector<int>>[NODES]; 

    int path[7];
    bool *onStack = new bool[nodeNum];
    memset(onStack, false, nodeNum*sizeof(bool));

    bool *reach1 = new bool[nodeNum];
    memset(reach1, false, nodeNum*sizeof(bool));

    bool *reach2 = new bool[nodeNum];
    memset(reach2, false, nodeNum*sizeof(bool));


    // system_clock::time_point startRead = system_clock::now();
    // system_clock::time_point endRead = system_clock::now();
    // microseconds duration = duration_cast<microseconds>(endRead - startRead);
    int *it1, *it2, *it3, *it, *next1, *next2, *next3;
    int prev1_num, prev2_num, prev3_num;
    vector<int> temp;

    for(int i=50000-tid; i>=0; i-=ThreadNum){
    // for(int i=tid; i<50001; i+=ThreadNum ){
        if(Graph[i].next_num!= 0){
            prev1_num = Graph[i].prev_num;
            next1 = Graph[i].prevs;
            for(it1=next1+prev1_num-1; (*it1)>i && it1>=next1; --it1)
            {
                prev2_num = Graph[*it1].prev_num;
                next2 = Graph[*it1].prevs;
                for(it2=next2+prev2_num-1; (*it2)>i && it2>=next2; --it2)
                {
                    jump2[*it2][jump2_num[*it2]++] = *it1;
                    reach1[*it2] = true;
                    temp1[tmp1++] = *it2;
                    prev3_num = Graph[*it2].prev_num;
                    next3 = Graph[*it2].prevs;
                    // it3 = lower_bound(next3, next3+prev3_num, i);
                    for(it3=next3+prev3_num-1;  (*it3)>i && it3>=next3; --it3)
                    {
                        if(*it3 <= i) continue;
                        temp.push_back(*it2);
                        temp.push_back(*it1);
                        jump3[*it3].push_back(temp);
                        reach2[*it3] = true;
                        temp2[tmp2++] = *it3;
                        temp.clear();
                    }
                }
            }
            for(it=temp2; it!=temp2+tmp2; ++it)
                sort(jump3[*it].rbegin(), jump3[*it].rend());     
            DFS2(i, tid, path, onStack, reach1, reach2, jump2, jump2_num, jump3);
            for(it=temp1; it!=temp1+tmp1; ++it)
            {
                reach1[*it] = false;
                jump2_num[*it] = 0;
            }
            for(it=temp2; it!=temp2+tmp2; ++it)
            {
                reach2[*it] = false;
                jump3[*it].clear();
            }
            tmp1 = 0;
            tmp2 = 0;
        }
    }
    delete temp1, temp2;
    delete onStack;

    delete[] jump3;
    delete reach1, reach2;
}

void multi_findCyle()
{
    int num_th = nodeNum / ThreadNum;
    thread vec_threads[ThreadNum];
    for(int i = 0; i < ThreadNum; ++i)
    {
        thread th(single_findCycle, i);
        vec_threads[i] = std::move(th);
    }
    for (int i=0; i<ThreadNum; ++i)
        (vec_threads[i]).join();
}


// buf write
void save_results(const string &outputFile){
    int buf_size = 200000;
    // char res[buf_size];
    char *res = new char[buf_size];
    char *cur = res;
    memset(res, '\0', buf_size);
    char *str;
    int len, write_num=0;
    int fp = open(outputFile.c_str(), O_CREAT|O_RDWR|O_TRUNC, 0666);
    // FILE *fp = fopen(outputFile.c_str(), "wb");
    for(int j=0; j<5; j++)
        for(int i=0; i<ThreadNum; ++i)
            ansNum += ansnum[j][i];
    str = (char*)(to_string(ansNum) + "\n").c_str();
    len = strlen(str);
    memcpy(cur, str, len);
    cur += len;
    struct Path* ans_all[ThreadNum];
    // system_clock::time_point startRead, endRead;
    // microseconds duration;
    for(int i=DEPTH_LOW-3; i<=DEPTH_HIGH-3; i++) {
        for(int k=0; k<ThreadNum; ++k)
            ans_all[k] = ans[i][k]+ansnum[i][k]-1;
        int ans_th_num = 0;
        int head = 0;
        int tid, sz, *path;
        while(head <= 50000)
        {
            tid = (50000-head) & (ThreadNum-1);
            // tid = (50000-head) % ThreadNum;  //0, 1, 2, 3
            while(ansnum[i][tid]>0 && ans_all[tid]->path[0] == head)
            {
                ++write_num;
                path = ans_all[tid]->path;
                sz = ans_all[tid]->len;
                for(; path!=ans_all[tid]->path+sz-1;)
                {
                    str = idmid[*path];
                    len = idlen[*path++];
                    memcpy(cur, str, len);
                    cur += len;
                }
                --ans_all[tid];
                --ansnum[i][tid];
                string str1 = idmid[*path];
                str = (char*)str1.c_str();
                len = idlen[*path++];
                str[len-1] = '\n';
                memcpy(cur, str, len);
                cur += len;
                if(write_num % 2000 == 1999)
                {
                    write(fp, res, cur-res);
                    cur = res;
                }
            }
            ++head;
        }
    }
    write(fp, res, cur-res);
    close(fp);
    delete res;
    // fwrite(res, cur-res, sizeof(char), fp);
    // fclose(fp);
}

int main()
{
    // auto startRead = system_clock::now();
    readData(testFile);
    // auto endRead = system_clock::now();
    // auto duration = duration_cast<microseconds>(endRead - startRead);
    // cout <<  "Read data cost: " << double(duration.count()) * microseconds::period::num / microseconds::period::den << " s." << endl;
    // startRead = system_clock::now();
    buildGraph();
    // endRead = system_clock::now();
    // duration = duration_cast<microseconds>(endRead - startRead);
    // cout <<  "buildGraph cost: " << double(duration.count()) * microseconds::period::num / microseconds::period::den << " s." << endl;
    // auto startTime = system_clock::now();
    for(int i=0; i<nodeNum; ++i)
        if(Graph[i].next_num != 0)
        {
            sort(Graph[i].nexts, Graph[i].nexts+Graph[i].next_num);
            sort(Graph[i].prevs, Graph[i].prevs+Graph[i].prev_num);
        }
    // auto endTime = system_clock::now();
    // duration = duration_cast<microseconds>(endTime - startTime);
    // cout <<  "Sort nexts and prevs cost: " << double(duration.count()) * microseconds::period::num / microseconds::period::den << " s." << endl;

    // startTime = system_clock::now();
    multi_findCyle();
    // endTime = system_clock::now();
    // duration = duration_cast<microseconds>(endTime - startTime);
    // cout <<  "find cycle cost: " << double(duration.count()) * microseconds::period::num / microseconds::period::den << " s." << endl;
    // startTime = system_clock::now();
    save_results(outputFile);
    // endTime = system_clock::now();
    // duration = duration_cast<microseconds>(endTime - startTime);
    // cout <<  "Write data cost: " << double(duration.count()) * microseconds::period::num / microseconds::period::den << " s." << endl;
    return 0;
}