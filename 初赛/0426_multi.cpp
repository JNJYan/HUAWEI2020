/*
 * @Author: JNJYan
 * @LastEditors: JNJYan
 * @Email: jjy20140825@gmail.com
 * @Date: 2020-04-13 10:32:13
 * @LastEditTime: 2020-04-27 16:15:48
 * @Description: Modify here please
 * @FilePath: /github/HUAWEI2020/0426_multi.cpp
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
#define NODES 300000
#define ThreadNum 4  
// #define TEST

string curDir = ".";
// string testFile = curDir + "/data/351244.txt";
string testFile = curDir + "/data/test_data2.txt";
string outputFile = curDir + "/projects/student/result1.txt";
mutex mtx;

struct Path{
    int len;
    int path[7];
    // char path_str[];
    Path(){};
    Path(int length, int *p){
        len = length;
        memcpy(path, p, len*sizeof(int));
        // for(int i=0; i<length; i++)

    };
};

struct Next1s{
    int next_num;
    int prev_num;
    int nexts[50];
    int prevs[250];
};

struct Next1s Graph[NODES];
string idmid[NODES];
int inputs[560000];
int *t = inputs;

struct Path ans[5][ThreadNum][1400000];

int ansnum[5][ThreadNum];
int nodeNum = 300000;
int ansNum = 0;

bool isNode[NODES];


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
                // inputs.push_back(x);
                // inputs[cnt++] = x;
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
    // auto startTime = system_clock::now();

    for(int i=0; i<NODES; ++i)
    {
        if(isNode[i])
            idmid[i] = to_string(i)+',';
    }
    // auto endTime = system_clock::now();
    // auto duration = duration_cast<microseconds>(endTime - startTime);
    // cout <<  "convert id to str cost: " << double(duration.count()) * microseconds::period::num / microseconds::period::den << " s." << endl;
    int v, w;
    for(auto p=inputs; p!=t;)
    {
        v = *p++;
        w = *p++;
        Graph[v].nexts[Graph[v].next_num++] = w;
        Graph[w].prevs[Graph[w].prev_num++] = v;
    }
    // for(auto p=inputs; p!=t;)
    // {
    //     v = *p++;
    //     w = *p++;
    //     Graph[w].prevs[Graph[w].prev_num++] = v;
    // }

    // for(int i=0; i<inputs.size();){
    //     v = inputs[i];
    //     w = inputs[i+1];
    //     // v = idHash[inputs[i]];
    //     // w = idHash[inputs[i+1]];
    //     Graph[v].nexts[Graph[v].next_num++] = w;
    //     Graph[w].prevs[Graph[w].prev_num++] = v;
    //     // ++outDegree[v];
    //     // ++inDegree[w];
    //     i += 2;
    // }
}

void delNode(int* degree){
    queue<int> que;
    int v, w;
    int *it;
    for(int i=0; i<nodeNum; ++i)
        if(degree[i] == 0)
            que.push(i);
    while(!que.empty())
    {
        v = que.front();
        que.pop();  
        for(it=Graph[v].nexts; it < Graph[v].nexts+Graph[v].next_num; ++it)
        {
            w = *it;
            if(--degree[w] == 0)
                que.push(w);
        }
        Graph[v].next_num = 0;
        Graph[v].prev_num = 0;
    }
}

void DFS(int head,
         int tid,
         int* path, 
         bool* onStack, 
         bool* reach1, 
         bool* reach2, 
         int* stack_dfs, 
         vector<int>* jump2,
         vector<vector<int>>* jump3){
    int* st_cur = stack_dfs;
    *st_cur++ = head;
    *st_cur++ = 1;
    int last_depth = 0, depth, cur, next_num, i;
    int *nexts, *p, *it, *pp;
    while(st_cur != stack_dfs){
        depth = *(--st_cur);
        cur = *(--st_cur);
        for (int i=last_depth; i>=depth; --i)
            onStack[path[i-1]] = false;
        onStack[cur] = true;
        path[depth-1] = cur;
        last_depth = depth;
        nexts = Graph[cur].nexts;
        next_num = Graph[cur].next_num;
        p = lower_bound(nexts, nexts+next_num, head);
        if(p-nexts != next_num && *p == head && depth >= DEPTH_LOW && depth < DEPTH_HIGH-1)
        {
            ans[depth-3][tid][ansnum[depth-3][tid]++] = Path(depth, path);
            // ans[tid][depth-3][ansnum[depth-3][tid]++] = Path(depth, path);
        }
        if(depth < DEPTH_HIGH - 2)
        {
            for(pp = nexts+next_num-1; *pp >= head && pp >=nexts; --pp){
                if(!onStack[*pp]){
                    *st_cur++ = *pp;
                    *st_cur++ = depth+1;
                }
            }
        }
        else if(reach1[cur] && (depth==DEPTH_HIGH-2))
        {
            for(auto &node: jump2[cur]){
                if(onStack[node])
                    continue;
                path[5] = node;
                ans[3][tid][ansnum[3][tid]++] = Path(6, path);
            }
        }
        if(reach2[cur] && (depth==DEPTH_HIGH-2))
        {
            for(auto &node: jump3[cur]){
                if(onStack[node[0]] || onStack[node[1]])
                    continue;
                path[5] = node[0];
                path[6] = node[1];
                ans[4][tid][ansnum[4][tid]++] = Path(7, path);
            }
        }
    }
    for(i=0;i<last_depth; i++)
        onStack[path[i]] = false;
}

void single_findCycle(int tid){
    int *temp1 = new int[nodeNum];
    int *temp2 = new int[nodeNum];
    int *st = new int[nodeNum*2];
    int tmp1 = 0, tmp2=0;
    // int temp[nodeNum], tmp=0;
    vector<int> jump2[NODES];
    vector<vector<int>>* jump3 = new vector<vector<int>>[NODES];
    // cout << tid << endl;
    // int st[nodeNum*2];
    int path[7];
    // cout << path << endl;
    bool *onStack = new bool[nodeNum];
    memset(onStack, false, nodeNum*sizeof(bool));

    bool *reach1 = new bool[nodeNum];
    memset(reach1, false, nodeNum*sizeof(bool));

    bool *reach2 = new bool[nodeNum];
    memset(reach2, false, nodeNum*sizeof(bool));
    
    int *it1, *it2, *it3, *it, *next1, *next2, *next3;
    int prev1_num, prev2_num, prev3_num;
    for(int i=tid; i<=nodeNum; i+=ThreadNum ){
        if(Graph[i].next_num!= 0){
            prev1_num = Graph[i].prev_num;
            next1 = Graph[i].prevs;
            it1 = lower_bound(next1, next1+prev1_num, i);
            for(; it1!=next1+prev1_num; ++it1)
            {
                if(*it1 <= i) continue;
                prev2_num = Graph[*it1].prev_num;
                next2 = Graph[*it1].prevs;
                it2 = lower_bound(next2, next2+prev2_num, i);
                for(; it2!=next2+prev2_num; ++it2)
                {
                    if(*it2 <= i) continue;
                    jump2[*it2].push_back(*it1);
                    reach1[*it2] = true;
                    temp1[tmp1++] = *it2;
                    prev3_num = Graph[*it2].prev_num;
                    next3 = Graph[*it2].prevs;
                    it3 = lower_bound(next3, next3+prev3_num, i);
                    for(; it3!=next3+prev3_num; ++it3)
                    {
                        if(*it3 <= i) continue;
                        vector<int> temp;
                        temp.push_back(*it2);
                        temp.push_back(*it1);
                        jump3[*it3].push_back(temp);
                        reach2[*it3] = true;
                        temp2[tmp2++] = *it3;
                    }
                }
            }
            for(it=temp1; it!=temp1+tmp1; ++it)
                sort(jump2[*it].begin(), jump2[*it].end());
            for(it=temp2; it!=temp2+tmp2; ++it)
                sort(jump3[*it].begin(), jump3[*it].end());
                // cout << *it << " : " << i <<endl;
                // for(auto aa:jump3[*it])
                // {
                //     for(auto a: aa)
                //         cout << a << ", ";
                //     cout << endl;
                // }
                // cout << endl;

            DFS(i, tid, path, onStack, reach1, reach2, st, jump2, jump3);
            for(it=temp1; it!=temp1+tmp1; ++it)
            {
                reach1[*it] = false;
                jump2[*it].clear();
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
    delete st;
    delete onStack;
    delete reach1, reach2;
}

void multi_findCyle()
{
    int num_th = nodeNum / ThreadNum;
    vector<thread> vec_threads;
    for(int i = 0; i < ThreadNum; ++i)
    {
        thread th(single_findCycle, i);
        vec_threads.emplace_back(std::move(th));
    }
    auto it = vec_threads.begin();
    for (; it != vec_threads.end(); ++it)
        (*it).join();
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
            ans_all[k] = ans[i][k];
        int ans_th_num = 0;
        int head = 0;
        int tid, sz, *path;
        while(head < nodeNum)
        {
            tid = head & (ThreadNum-1);
            // tid = head % ThreadNum;
            while(ansnum[i][tid]>0 && ans_all[tid]->path[0] == head)
            {
                ++write_num;
                path = ans_all[tid]->path;
                sz = ans_all[tid]->len;
                for(; path!=ans_all[tid]->path+sz-1;)
                {
                    str = (char*)idmid[*path++].c_str();
                    len = strlen(str);
                    memcpy(cur, str, len);
                    cur += len;
                }
                ++ans_all[tid];
                --ansnum[i][tid];
                string str1 = idmid[*path++];
                str = (char*)str1.c_str();
                len = strlen(str);
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
    for(int i=0; i<nodeNum; ++i)
        if(Graph[i].next_num != 0)
        {
            sort(Graph[i].nexts, Graph[i].nexts+Graph[i].next_num);
            sort(Graph[i].prevs, Graph[i].prevs+Graph[i].prev_num);
        }
    // auto startTime = system_clock::now();
    multi_findCyle();
    // auto endTime = system_clock::now();
    // duration = duration_cast<microseconds>(endTime - startTime);
    // cout <<  "find cycle cost: " << double(duration.count()) * microseconds::period::num / microseconds::period::den << " s." << endl;

    // startTime = system_clock::now();
    save_results(outputFile);
    // endTime = system_clock::now();
    // duration = duration_cast<microseconds>(endTime - startTime);
    // cout <<  "Write data cost: " << double(duration.count()) * microseconds::period::num / microseconds::period::den << " s." << endl;
    return 0;
}