/*
 * @Author: JNJYan
 * @LastEditors: JNJYan
 * @Email: jjy20140825@gmail.com
 * @Date: 2020-04-13 10:32:13
 * @LastEditTime: 2020-04-27 16:01:02
 * @Description: Modify here please
 * @FilePath: /github/HUAWEI2020/0424.cpp
 */
#include <bits/stdc++.h>
#include <unistd.h>
#include <fcntl.h>
#include <string>
#include <thread>
#include <algorithm>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
using namespace std;

#define DEPTH_HIGH 7
#define DEPTH_LOW 3
#define NODES 280000
// #define TEST

string curDir = ".";
string testFile = curDir + "/data/351244.txt";
// string testFile = curDir + "/data/test_data_10000_60000.txt";
string outputFile = curDir + "/projects/student/result.txt";

struct Next1s{
    int next_num;
    int prev_num;
    int nexts[50];
    int prevs[250];
};

struct Next1s Graph[NODES];
unordered_map<int, int> idHash;
string idmid[NODES];
vector<int> inputs;
int inDegree[NODES];
int outDegree[NODES];
vector<int> jump2[NODES];

int ans3[3*500000];
int ans4[4*500000];
int ans5[5*1000000];
int ans6[6*2000000];
int ans7[7*3500000];
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
        // idmid[x] = to_string(x)+',';
        idmid[nodeNum] = to_string(x) + ',';
        idHash[x] = nodeNum++;
    }
    int v, w;
    for(int i=0; i<inputs.size();){
        // v = inputs[i];
        // w = inputs[i+1];
        v = idHash[inputs[i]];
        w = idHash[inputs[i+1]];
        Graph[v].nexts[Graph[v].next_num++] = w;
        Graph[w].prevs[Graph[w].prev_num++] = v;
        // ++outDegree[v];
        // ++inDegree[w];
        i += 2;
    }
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


void DFS(int head, int* path, bool* onStack, bool* reach, int* stack_dfs){
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
        // p = nexts;
        // while(*p<head)
        //     p++;
        if(p-nexts != next_num && *p == head && depth >= DEPTH_LOW && depth < DEPTH_HIGH)
        {
            for(it=path; it!=path+depth; ++it)
                *ans[depth-3]++ = *it;
            ++ansNum;
        }
        if(depth < DEPTH_HIGH - 1)
        {
            for(pp = nexts+next_num-1; *pp >= head && pp >=nexts; --pp){
                if(!onStack[*pp]){
                    *st_cur++ = *pp;
                    *st_cur++ = depth+1;
                }
            }
        }else if(reach[cur] && depth == DEPTH_HIGH - 1)
        {
            for(auto &node: jump2[cur]){
                if(onStack[node])
                    continue;
            for(it=path; it!=path+depth; ++it)
                *ans[4]++ = *it;
            *ans[4]++ = node;
            ++ansNum;
            }
        }
    }
    for(i=0;i<last_depth; i++)
        onStack[path[i]] = false;
}

void single_findCycle(){
    int path[7];
    int temp[nodeNum], tmp=0;
    int st[nodeNum*2];

    bool onStack[nodeNum];
    memset(onStack, false, nodeNum*sizeof(bool));

    bool reach[nodeNum];
    memset(reach, false, nodeNum*sizeof(bool));

    int *it1, *it2, *it, *next1, *next2;
    int prev1_num, prev2_num;

    for(int i=0; i<nodeNum; i++){
        // cout << i <<endl;
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
                    reach[*it2] = true;
                    temp[tmp++] = *it2;
                }
            }
            for(it=temp; it!=temp+tmp; ++it)
                sort(jump2[*it].begin(), jump2[*it].end());

            DFS(i, path, onStack, reach, st);
            for(it=temp; it!=temp+tmp; ++it)
            {
                reach[*it] = false;
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
    char *str;
    int len, write_num;

    FILE *fp = fopen(outputFile.c_str(), "wb");
    str = (char*)(to_string(ansNum) + "\n").c_str();
    len = strlen(str);
    memcpy(cur, str, len);
    cur += len;

    for(int i=DEPTH_LOW-3; i<=DEPTH_HIGH-3; i++) {
        for(auto x=ans_out[i]; x!=ans[i];) {
            write_num++;
            for(int j=0; j<i+2; j++){
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
    // delNode(inDegree);
    // delNode(outDegree);
    for(int i=0; i<nodeNum; ++i)
        if(Graph[i].next_num != 0)
        {
            sort(Graph[i].nexts, Graph[i].nexts+Graph[i].next_num);
            sort(Graph[i].prevs, Graph[i].prevs+Graph[i].prev_num);
        }
    single_findCycle();
    clock_t startTime = clock();
    save_results(outputFile);
    clock_t endTime = clock();
    cout << "write data cost " << (endTime - startTime) / (float)CLOCKS_PER_SEC << endl;
#ifdef TEST
    cout << "Total Loops " << ansNum;
#endif
    return 0;
}