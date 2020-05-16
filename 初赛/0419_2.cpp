/*
 * @Author: JNJYan
 * @LastEditors: JNJYan
 * @Email: jjy20140825@gmail.com
 * @Date: 2020-04-13 10:32:13
 * @LastEditTime: 2020-04-23 19:10:45
 * @Description: Modify here please
 * @FilePath: /preliminary/0419_2.cpp
 */
#include <bits/stdc++.h>
#include <unistd.h>
#include <fcntl.h>
#include <string>
#include <thread>
#include <mutex>
#include <algorithm>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
using namespace std;

#define DEPTH_HIGH 7
#define DEPTH_LOW 3
#define NODES 220000
// #define TEST

string curDir = ".";
// string testFile = curDir + "/data/351244.txt";
string testFile = curDir + "/data/test_data.txt";
string outputFile = curDir + "/projects/student/result.txt";

struct Next1s{
    int next_num;
    int prev_num;
    int nexts[50];
    int prevs[250];
    // Nexts(int num, int* nexts): num(0), nexts(){}
};

struct Next1s Graph[NODES];
unordered_map<int, int> idHash;
string idmid[NODES];
vector<int> inputs;
int inDegree[NODES];
int outDegree[NODES];
// vector<unordered_map<int, vector<int>>> jump2;
// vector<Path> ans[8];
vector<int> jump2[NODES];

int ans3[3*500000];
int ans4[4*500000];
int ans5[5*1000000];
int ans6[6*2000000];
int ans7[7*3000000];
int* ans3_idx = ans3;
int* ans4_idx = ans4;
int* ans5_idx = ans5;
int* ans6_idx = ans6;
int* ans7_idx = ans7;

int* ans[] = {ans3, ans4, ans5, ans6, ans7};
int* ans_out[] = {ans3_idx, ans4_idx, ans5_idx, ans6_idx, ans7_idx};

int nodeNum = 0;
int ansNum = 0;

void readData(string &TestFile){
    int fp = open(TestFile.c_str(), O_RDONLY);
    int len = lseek(fp, 0, SEEK_END);
    char* mbuf = (char *)mmap(NULL, len, PROT_READ, MAP_PRIVATE, fp, 0);
    char* p = mbuf;
    int x;
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
                inputs.push_back(x);
                ++p;
            }
            else
                while(*p++ != '\n');
        }
    }
}

void buildGraph(){
    auto tmp=inputs;
    sort(tmp.begin(), tmp.end());
    tmp.erase(unique(tmp.begin(), tmp.end()), tmp.end());
    for(int &x:tmp){
        idmid[nodeNum] = to_string(x)+',';
        idHash[x] = nodeNum++;
    }
    int v, w;
    for(int i=0; i<inputs.size();){
        v = idHash[inputs[i]];
        w = idHash[inputs[i+1]];
        Graph[v].nexts[Graph[v].next_num++] = w;
        Graph[w].prevs[Graph[w].prev_num++] = v;
        ++outDegree[v];
        ++inDegree[w];
        i += 2;
    }
    for(int i=0; i<nodeNum; ++i)
        sort(Graph[i].prevs, Graph[i].prevs+Graph[i].prev_num);
}


void delNode(int* degree){
    queue<int> que;
    for(int i=0; i<nodeNum; ++i)
        if(degree[i] == 0)
            que.push(i);
    int v, w;
    while(!que.empty())
    {
        v = que.front();
        que.pop();
        for(auto it=Graph[v].nexts; it < Graph[v].nexts+Graph[v].next_num; ++it)
        {
            w = *it;
            if(--degree[w] == 0)
                que.push(w);
        }
    }
}

void DFS(int head,
         int cur,
         int depth,
         int* path,
         bool* onStack,
         int* reach){
    onStack[cur]=true;
    path[depth-1] = cur;
    int* nexts = Graph[cur].nexts;
    auto p = lower_bound(nexts, nexts+Graph[cur].next_num, head);
    if(p-nexts!=Graph[cur].next_num && *p == head && depth >= DEPTH_LOW && depth < DEPTH_HIGH)
    {
        for(auto it=path; it!=path+depth; ++it)
            *ans[depth-3]++ = *it;
        ++ansNum;
    }
    if(depth < DEPTH_HIGH - 1)
    {
        for(;p-nexts!=Graph[cur].next_num; ++p){
            if(!onStack[*p])
                DFS(head, *p, depth+1, path, onStack, reach);
        }
    }else if(reach[cur] > -1 && depth == DEPTH_HIGH - 1)
    {
        auto path_node = jump2[cur];
        for(auto &node:path_node){
            if(onStack[node])
                continue;
            for(auto it=path; it!=path+depth; ++it)
                *ans[4]++ = *it;
            *ans[4]++ = node;
            ++ansNum;
        }
    }
    onStack[cur] = false;
    path[depth-1] = 0;
}


void single_findCycle(){
    int path[7];
    int temp[nodeNum], tmp=0;
    bool onStack[nodeNum];
    memset(onStack, false, nodeNum*sizeof(bool));
    int reach[nodeNum];
    memset(reach, -1, nodeNum*sizeof(int));
    int path1_idx, path2_idx, start;
    int *it1, *it2;
    for(int i=0; i<nodeNum; i++){
        if(Graph[i].next_num!= 0){
        // for(int i=0; i<5; ++i)
        // {
        //     cout << i+1 << ":" << (ans[i]- ans_out[i]) / (i+3) << "||";
        // }
        // cout << i << ":" << ansNum << endl;
            it1 = lower_bound(Graph[i].prevs, Graph[i].prevs+Graph[i].prev_num, i);
            for(; it1!=Graph[i].prevs+Graph[i].prev_num; ++it1)
            {
                if(*it1 <= i) continue;
                it2 = lower_bound(Graph[*it1].prevs, Graph[*it1].prevs+Graph[*it1].prev_num, i);
                for(; it2!=Graph[*it1].prevs+Graph[*it1].prev_num; ++it2)
                {
                    if(*it2 <= i) continue;
                    jump2[*it2].push_back(*it1);
                    reach[*it2] = 1;
                    temp[tmp++] = *it2;
                }
            }
            for(auto it=temp; it!=temp+tmp; ++it)
                sort(jump2[*it].begin(), jump2[*it].end());

            DFS(i, i, 1, path, onStack, reach);
            for(auto it=temp; it!=temp+tmp; ++it)
            {
                reach[*it] = -1;
                jump2[*it].clear();
            }
            tmp = 0;
        }
    }
}


// buf fwrite
void save_results(const string &outputFile){
    char res[200000];
    char *cur = res;
    memset(res, '\0', 200000);
    FILE *fp = fopen(outputFile.c_str(), "wb");
    char *str;
    int len;
    str = (char*)(to_string(ansNum) + "\n").c_str();
    len = strlen(str);
    memcpy(cur, str, len);
    cur += len;
    int write_num = 0;
    int ch_num = 0;
    for(int i=DEPTH_LOW-3;i<=DEPTH_HIGH-3;i++) {
        for(auto x=ans_out[i]; x!=ans[i];) {
            write_num++;
            for(int j=0;j<i+2;j++){
                str = (char*)idmid[*x++].c_str();
                len = strlen(str);
                memcpy(cur, str, len);
                cur += len;
            }
            string str1 = idmid[*x++];
            str = (char*)str1.c_str();
            len = strlen(str);
            str[len-1] = '\n';
            memcpy(cur, str, len);
            cur += len;
            if(write_num % 2000 == 1999)
            {
                fwrite(res, cur-res , sizeof(char), fp);
                cur = res;
            }
        }
    }
    fwrite(res, cur-res, sizeof(char), fp);
    fclose(fp);
}

int main()
{
    readData(testFile);
    buildGraph();
    delNode(inDegree);
    delNode(outDegree);
    for(int i=0; i<nodeNum; ++i)
        if(outDegree[i]!=0)
            sort(Graph[i].nexts, Graph[i].nexts+Graph[i].next_num);
        else
            Graph[i].next_num = 0;

    // genJump2st();
    // cout << "read data cost " << (endTime - startTime) / (float)CLOCKS_PER_SEC << endl;
    single_findCycle();
    // startTime = clock();
    // save_results5(outputFile);
    // endTime = clock();
    // save_fwrite(outputFile);

    // clock_t startTime = clock();
    save_results(outputFile);
    // clock_t endTime = clock();
    // cout << "write data cost " << (endTime - startTime) / (float)CLOCKS_PER_SEC << endl;
#ifdef TEST
    cout << "Total Loops " << ansNum;
#endif
    return 0;
}