/*
 * @Author: JNJYan
 * @LastEditors: JNJYan
 * @Email: jjy20140825@gmail.com
 * @Date: 2020-04-13 10:32:13
 * @LastEditTime: 2020-05-16 20:11:59
 * @Description: Modify here please
 * @FilePath: \second\05165.cpp
 */
#include <bits/stdc++.h>
#include <unistd.h>
#include <fcntl.h>
#include <string>
#include <thread>
#include <atomic>
#include <mutex>
#include <chrono>
#include <algorithm>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
using namespace std;
using namespace chrono;

#define DEPTH_HIGH 8
#define DEPTH_LOW 3
#define NODES 2000000
#define HASHSZ 5000000
#define ThreadNum 4
#define TEST

#define  likely(x)     __builtin_expect(!!(x), 1)
#define  unlikely(x)   __builtin_expect(!!(x), 0)


#ifdef TEST
string testFile = "./data/test_data4.txt";
// string testFile = "./data/351244.txt";
string outputFile = "./projects/student/result.txt";
#else
string testFile = "/data/test_data.txt";
string outputFile = "/projects/student/result.txt";
#endif

char idmid[NODES][12];
int idlen[NODES];

struct Path{
    int path[8];
    Path(){};
    Path(int length, int *p){
        memcpy(path, p, length*sizeof(int));
    };
};

struct Edge{
    int u;
    int64_t value;
    int64_t up;
    int64_t down;
    Edge(){};
    Edge(int u, int64_t value): u(u), value(value){
        up = value * 30;
        down = value * 2;
        this->value *= 10;
    };
    bool operator<(const Edge& b)const{
        return u < b.u;
    };
};

struct Next1s{
    struct Edge* nexts;
    struct Edge* prevs;
};

struct jump2path{
    int path;
    int64_t start;
    int64_t end_down;
    int64_t end_up;
    bool operator<(const jump2path& b)const{
        return path < b.path;
    };
};

struct Hash{
    int key;
    int value;
    Hash(){
        key = -1;
        value = -1;
    };
};

int searchid[NODES];
int search_cnt = 0;
struct Next1s Graph[NODES];
int inputs[4000000];
int inputs_cnt;
int64_t money[2000000];
int money_cnt;

int next_num[NODES];
int prev_num[NODES];

vector<struct Path> ans[6][NODES];
int ansnum[6][ThreadNum];
int nodeNum = 0;

atomic<int> outdegree[NODES];
atomic<int> indegree[NODES];

struct Hash appear[HASHSZ];
struct Hash idHash[NODES*2];

void readData(string &testFile)
{
#ifdef TEST
    auto t1 = system_clock::now();
#endif
    int fp = open(testFile.c_str(), O_RDONLY);
    struct stat info;
    stat(testFile.c_str(), &info);
    size_t len = info.st_size;
    char * buf = (char *) mmap(NULL, len, PROT_READ, MAP_PRIVATE, fp, 0);
    int start, end;
    int64_t  weight;
    int little;
    do {
        start = 0;
        while (*buf != ',') {
            start = (start << 3) + (start << 1) + (*buf ^ 48);
            buf++;
        }
        buf++;
        end = 0;
        while (*buf != ',') {
            end = (end << 3) + (end << 1) + (*buf ^ 48);
            buf++;
        }
        buf++;
        weight = 0;
        little = 0;
        while (*buf != '\r' && *buf != '\n') {
            if(*buf == '.')
            {
                buf++;
                int cnt = 0;
                while (*buf != '\r' && *buf != '\n'){
                    little = (little << 3) + (little << 1) + (*buf ^ 48);
                    cnt++;
                    buf++;
                }
                buf++;
                break;
            }
            weight = (weight << 3) + (weight << 1) + (*buf ^ 48);
            buf++;
        }
        weight = weight * 100 + little;
        if(*buf == '\r') buf++;
        buf++;
        inputs[inputs_cnt++] = start;
        inputs[inputs_cnt++] = end;
        money[money_cnt++] = weight;
    }while(*buf != '\0');

}

const char table[201] = "00010203040506070809101112131415161718192021222324252627282930313233343536373839404142434445464748495051525354555657585960616263646566676869707172737475767778798081828384858687888990919293949596979899";
void itoa(const int &nodeNum, int x)
{
    char aa[20];
    char *cur = idmid[nodeNum];
    int len = 0;
    while(x >= 100)
    {
        auto index = (x % 100) * 2;
        x /= 100;
        aa[len++] = table[index + 1];
        aa[len++] = table[index];
    }
    if(x>=10)
    {
        auto index = x * 2;
        aa[len++] = table[index + 1];
        aa[len++] = table[index];
    }

    if (x < 10)
        aa[len++] = table[2*x + 1];

    for(int j=len-1;j>=0;j--)
        *cur++ = aa[j];
    *cur++ = ',';
    idlen[nodeNum] = len+1;
}

bool node[5000000];
void CalDegree(const int &start, const int &end)
{
    int v, w;
    for(int i=start; i<end; i+=2)
    {
        v = inputs[i];
        w = inputs[i+1];
        ++outdegree[v];
        ++indegree[w];
    }
}

void newGraph(const int &start, const int &end){
    for(int i=start; i<end; i++)
    {
        Graph[i].nexts = new Edge[outdegree[i]];
        Graph[i].prevs = new Edge[indegree[i]];
    }
}
void CreateGraph(const int &start, const int &end){
    int64_t tmp_money;
    int  w, v;
    for(int i=start; i<end; i += 2){
        // if(node[i])
        //     continue;
        v = inputs[i];
        w = inputs[i+1];
        tmp_money = money[i/2];
        Graph[v].nexts[next_num[v]++] = Edge(w, tmp_money);
        Graph[w].prevs[prev_num[w]++] = Edge(v, tmp_money);
    }
}
int appears[NODES];
void buildGraph(){
    int index;
    int hash_gap = int(NODES / 7);
    int app=0;
    for(int i=0; i<inputs_cnt; ++i){
        index = inputs[i] % (HASHSZ);
        while(appear[index].key!=-1 && appear[index].key !=inputs[i]){
            index = (index+hash_gap)%(HASHSZ);
        }
        if(appear[index].key==-1){
            appear[index].key = inputs[i];
            appears[app++] = inputs[i];
        }
    }
    sort(appears, appears+app);

    int x;
    for(int i=0; i<app;++i){
        x = appears[i];
        itoa(nodeNum, x);
        index = x%(NODES*2);
        while(idHash[index].key!=-1){
            index = (index+hash_gap)%(NODES*2);
        }
        idHash[index].value = nodeNum;
        idHash[index].key = x;
        ++nodeNum;
    }
    int v, w;
    for(int i=0; i<inputs_cnt; i++){
        index = inputs[i] % (2*NODES);
        while(idHash[index].key!=inputs[i]){
            index = (index+hash_gap)%(NODES*2);
        }
        inputs[i] = idHash[index].value;
    }
    thread th[ThreadNum];
    int per_num = inputs_cnt / ThreadNum;
    // for(int tid=0; tid<ThreadNum; tid++)
    //     th[tid] = thread(CalDegree, tid * per_num, (tid + 1) * per_num);
    // for(int tid=0; tid<ThreadNum; tid++)
    //     th[tid].join();
    for(int i=0; i<inputs_cnt; i+=2)
    {
        v = inputs[i];
        w = inputs[i+1];
        outdegree[v]++;
        indegree[w]++;
    }
    for(int i=0; i<nodeNum; i++)
    {
        Graph[i].nexts = new Edge[outdegree[i]];
        Graph[i].prevs = new Edge[indegree[i]];
    }

#ifdef TEST
    auto startTime = system_clock::now();
#endif
    int64_t tmp_money;
    for(int i=0; i<inputs_cnt; i += 2){
        // if(node[i])
        //     continue;
        v = inputs[i];
        w = inputs[i+1];
        tmp_money = money[i/2];
        Graph[v].nexts[next_num[v]++] = Edge(w, tmp_money);
        Graph[w].prevs[prev_num[w]++] = Edge(v, tmp_money);
    }
    // per_num = inputs_cnt / ThreadNum;
    // for(int tid=0; tid<ThreadNum; tid++)
    //     th[tid] = thread(CreateGraph, tid * per_num, (tid + 1) * per_num);
    // for(int tid=0; tid<ThreadNum; tid++)
    //     th[tid].join();
#ifdef TEST
    auto endTime = system_clock::now();
    auto duration = duration_cast<microseconds>(endTime - startTime);
    cout <<  "build nexts and prevs cost: " << double(duration.count()) * microseconds::period::num / microseconds::period::den << " s." << endl;
#endif
}

int delnode[NODES];

void delNode(){
    int *del = delnode;
    int start = 0;
    int end = 0;
    int sz, v;
    
    for(int i=0; i<nodeNum; ++i)
        if(next_num[i] == 0)
            del[end++] = i;

    while(start != end)
    {
        v = del[start++];
        sz = prev_num[v];
        for(int i=0; i<sz; ++i){
            auto w = Graph[v].prevs[i];
            if(--outdegree[w.u] == 0)
                del[end++] = w.u;
        }
        // 若节点入度或出度为零，则该节点为头节点时不需要进行搜索
        next_num[v] = 0;
    }
#ifdef TEST
    cout << "Rmove outdegree is zero num : " << end << endl;
#endif
    start = 0;
    end = 0;
    for(int i=0; i<nodeNum; ++i)
        if(prev_num[i] == 0)
            del[end++] = i;

    while(start != end)
    {
        v = del[start++];
        sz = next_num[v];
        for(int i=0; i<sz; ++i){
            auto w = Graph[v].nexts[i];
            if(--indegree[w.u] == 0)
                del[end++] = w.u;
        }
        next_num[v] = 0;
    }
#ifdef TEST
    cout << "Rmove indegree is zero num : " << end << endl;
#endif
}

inline bool check(const Edge &a, const Edge &b)
{
    return a.down <= b.value && b.value <= a.up;
}
inline bool check(const Edge& a, const jump2path& node, const Edge& b)
{
    return a.down <= node.start && node.start <= a.up && node.end_down <= b.value && b.value <= node.end_up;
}
void DFS2(int head,
          int tid,
          int* path,
          bool* reach1,
          bool* reach2,
          bool* reach3,
          vector<struct jump2path>* jump2,
          int* jump2_num){
    int next1_val, next2_val, next3_val, next4_val, next5_val;
    path[0] = head;

    int t1, t2, t3, t4, t5, t6;
    int sz1 = next_num[head];
    Edge *n1 = Graph[head].nexts;
    for(int i1=0; i1<sz1; ++i1, ++n1)
    {
        Edge &next1 = *n1;
        next1_val = next1.u;
        if(next1_val < head)
            break;
        path[1] = next1_val;
        int sz2 = next_num[next1_val];
        Edge *n2 = Graph[next1_val].nexts;
        for(int i2=0; i2<sz2; ++i2, ++n2)
        {
            Edge& next2 = *n2;
            next2_val = next2.u;
            if(next2_val <= head)
                break;
            if(!check(next1, next2))
                continue;
            path[2] = next2_val;
            int sz3 = next_num[next2_val];
            Edge *n3 = Graph[next2_val].nexts;
            for(int i3=0; i3<sz3; ++i3, ++n3)
            {
                Edge& next3 = *n3;
                next3_val = next3.u;
                if(next3_val < head)
                    break;
                if(next3_val == next1_val || !check(next2, next3))
                    continue;
                if(next3_val == head){
                    if(!check(next3, next1))
                        continue;
                    ans[0][head].emplace_back(Path(3, path));
                    continue;
                }
                path[3] = next3_val;
                int sz4 = next_num[next3_val];
                Edge *n4 = Graph[next3_val].nexts;
                for(int i4=0; i4<sz4; ++i4, ++n4)
                {
                    Edge& next4 = *n4;
                    int next4_val = next4.u;
                    if(next4_val < head)
                        break;
                    // if(!reach3[next4_val] || !reach2[next4_val] && !reach1[next4_val] && next4_val != head)
                    //     continue;
                    if (next4_val == next1_val || next4_val == next2_val || !check(next3, next4))
                        continue;
                    if(next4_val == head){
                        if(!check(next4, next1))
                            continue;
                        ans[1][head].emplace_back(Path(4, path));
                        continue;
                    }
                    path[4] = next4_val;
                    if(reach1[next4_val])
                    {
                        for(int i=0; i<jump2_num[next4_val]; ++i){
                            auto &node = jump2[next4_val][i];
                            if(node.path == next1_val || node.path == next2_val || node.path == next3_val ||  !check(next4, node, next1))
                                continue;
                            path[5] = node.path;
                            ans[3][head].emplace_back(Path(6, path));
                         }
                    }
                    int sz5 = next_num[next4_val];
                    Edge *n5 = Graph[next4_val].nexts;
                    for(int i5=0; i5<sz5; ++i5, ++n5)
                    {
                        Edge& next5 = *n5;
                        int next5_val = next5.u;
                        if(next5_val < head)
                            break;
                        if (next5_val == next1_val || next5_val == next2_val || next5_val == next3_val || next5_val == next4_val || !check(next4, next5))
                            continue;
                        if(next5_val == head){
                            if(!check(next5, next1))
                                continue;
                            ans[2][head].emplace_back(Path(5, path));
                            continue;
                        }
                        path[5] = next5_val;
                        if(reach1[next5_val])
                        {
                            for(int i=0; i<jump2_num[next5_val]; ++i){
                                auto &node = jump2[next5_val][i];
                                if(node.path == next1_val || node.path == next2_val || node.path == next3_val || node.path == next4_val || !check(next5, node, next1))
                                    continue;
                                path[6] = node.path;
                                ans[4][head].emplace_back(Path(7, path));
                            }
                        }
                        if(reach2[next5_val])
                        {
                            int sz6 = next_num[next5_val];
                            Edge *n6 = Graph[next5_val].nexts;
                            for(int i6=0; i6<sz6; ++i6, ++n6)
                            {
                                Edge& next6 = *n6;
                                int next6_val = next6.u;
                                if(next6_val < head)
                                    break;
                                if(next6_val == next1_val || next6_val == next2_val || next6_val == next3_val || next6_val == next4_val || !check(next5, next6))
                                    continue;
                                path[6] = next6_val;
                                if(reach1[next6_val])
                                {
                                    for(int i=0; i<jump2_num[next6_val]; ++i){
                                        auto &node = jump2[next6_val][i];
                                        if(node.path == next1_val || node.path == next2_val || node.path == next3_val || node.path == next4_val || node.path == next5_val|| !check(next6, node, next1))
                                            continue;
                                        path[7] = node.path;
                                        ans[5][head].emplace_back(Path(8, path));
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    for(int i=0; i<6; i++)
        ansnum[i][tid] += ans[i][head].size();
}

void single_findCycle(const int &tid){
    int *reset1 = new int[nodeNum];
    int tmp1 = 0;
    int *reset2 = new int[nodeNum];
    int tmp2 = 0;
    int *reset3 = new int[nodeNum];
    int tmp3 = 0;

    int jump2_num[nodeNum];
    memset(jump2_num, 0, nodeNum*sizeof(int));
    vector<struct jump2path>* jump2 = new vector<struct jump2path>[nodeNum];

    int path[8];

    bool *reach1 = new bool[nodeNum];
    memset(reach1, false, nodeNum*sizeof(bool));

    bool *reach2 = new bool[nodeNum];
    memset(reach2, false, nodeNum*sizeof(bool));

    bool *reach3 = new bool[nodeNum];
    memset(reach3, false, nodeNum*sizeof(bool));

#ifdef TEST
    auto startTime = system_clock::now();
    auto endTime = system_clock::now();
    auto duration = duration_cast<microseconds>(endTime - startTime);
#endif
    int it1, it2, it3, it4, it;
    int t1, t2, t3, t4, t5;
    struct jump2path temp_jump2;
    for(int i=tid; i<nodeNum; i+=ThreadNum){
        if(next_num[i]!= 0){
            const int &sz1 = prev_num[i];
            for(int i1=0; i1<sz1; i1++)
            {
                const auto &prev1 = Graph[i].prevs[i1];
                if(prev1.u <= i)
                    break;
                const int &sz2 = prev_num[prev1.u];
                for(int i2=0; i2<sz2; i2++)
                {
                    const auto &prev2 = Graph[prev1.u].prevs[i2];
                    if(prev2.u <= i)
                        break;
                    if(!check(prev2, prev1))
                        continue;
                    temp_jump2.path = prev1.u;
                    temp_jump2.start = prev2.value;
                    temp_jump2.end_down = prev1.down;
                    temp_jump2.end_up = prev1.up;
                    jump2[prev2.u].emplace_back(temp_jump2);
                    jump2_num[prev2.u]++;
                    reach1[prev2.u] = true;
                    reset1[tmp1++] = prev2.u;
                    const int &sz3 = prev_num[prev2.u];
                    for(int i3=0; i3<sz3; i3++)
                    {
                        const auto &prev3 = Graph[prev2.u].prevs[i3];
                        if(prev3.u <= i)
                            break;
                        if(!check(prev3, prev2) || prev3.u==prev1.u)
                            continue;
                        reach2[prev3.u] = true;
                        reset2[tmp2++] = prev3.u;
                        const int &sz4 = prev_num[prev3.u];
                        for(int i4=0; i4<sz4; i4++)
                        {
                            const auto &prev4 = Graph[prev3.u].prevs[i4];
                            if(prev4.u <= i)
                                break;
                            if(!check(prev4, prev3) || prev4.u==prev1.u)
                                continue;
                            reach3[prev4.u] = true;
                            reset3[tmp3++] = prev4.u;
                        }
                    }
                }
            }

#ifdef TEST
    startTime = system_clock::now();
#endif
            DFS2(i, tid, path, reach1, reach2, reach3, jump2, jump2_num);

#ifdef TEST
    endTime = system_clock::now();
    duration += duration_cast<microseconds>(endTime - startTime);
#endif
            for(auto it=reset1; it!=reset1+tmp1; ++it)
            {
                reach1[*it] = false;
                jump2_num[*it] = 0;
                jump2[*it].resize(0);
            }
            for(auto it=reset2; it!=reset2+tmp2; ++it)
                reach2[*it] = false;
            for(auto it=reset3; it!=reset3+tmp3; ++it)
                reach3[*it] = false;
            tmp1 = 0;
            tmp2 = 0;
            tmp3 = 0;
        }
    }
    delete reset1, reset2, reset3;
    delete[] jump2;
    delete reach1, reach2, reach3;
#ifdef TEST
    cout <<  "search cost: " << double(duration.count()) * microseconds::period::num / microseconds::period::den << " s." << endl;
#endif
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


void save_results(const string &outputFile){
    int buf_size = 200000;
    char *res = new char[buf_size];
    char *cur = res;
    memset(res, '\0', buf_size);
    char *str;
    int len, write_num=0;
    int fp = open(outputFile.c_str(), O_CREAT|O_RDWR|O_TRUNC, 0666);
    // FILE *fp = fopen(outputFile.c_str(), "wb");
    int ansNum;
    for(int i=0; i<6; i++)
        for(int j=0; j<ThreadNum; j++)
            ansNum += ansnum[i][j];
    cout << ansNum << endl;
    str = (char*)(to_string(ansNum) + "\n").c_str();
    len = strlen(str);
    memcpy(cur, str, len);
    cur += len;
    int node;
    // struct Path* ans_all[ThreadNum];
    // system_clock::time_point startRead, endRead;
    // microseconds duration;
    for(int i=DEPTH_LOW-3; i<=DEPTH_HIGH-3; i++) {
        int head = 0;
        int depth, sz, *path;
        depth = i+2;
        while(head <nodeNum)
        {
            for(auto head_path=ans[i][head].rbegin(); head_path!=ans[i][head].rend();++head_path)
            {
                ++write_num;
                path = (*head_path).path;
                sz = 0;
                while(sz<depth)
                {
                    node = path[sz];
                    str = (char*)idmid[node];
                    len = idlen[node];
                    memcpy(cur, str, len);
                    cur += len;
                    ++sz;
                }
                node = path[sz];
                string str1 = idmid[node];
                str = (char*)str1.c_str();
                len = idlen[node];
                str[len-1] = '\n';
                memcpy(cur, str, len);
                cur += len;
                if(write_num == 2000)
                {
                    write(fp, res, cur-res);
                    cur = res;
                    write_num = 0;
                }
            }
            ++head;
        }
    }
    write(fp, res, cur-res);
    close(fp);
    delete[] res;
}
bool cmp(Edge &a, Edge &b)
{
    return a.u > b.u;
}

int main()
{

#ifdef TEST
    auto buildStart = system_clock::now();
    auto startRead = system_clock::now();
#endif  
    readData(testFile);

#ifdef TEST
    auto endRead = system_clock::now();
    auto duration = duration_cast<microseconds>(endRead - startRead);
    cout <<  "Read data cost: " << double(duration.count()) * microseconds::period::num / microseconds::period::den << " s." << endl;
    startRead = system_clock::now();
#endif
    buildGraph();

#ifdef TEST
    endRead = system_clock::now();
    duration = duration_cast<microseconds>(endRead - startRead);
    cout <<  "buildGraph cost: " << double(duration.count()) * microseconds::period::num / microseconds::period::den << " s." << endl;
#endif

#ifdef TEST
    auto startTime = system_clock::now();
#endif
    //delete outdegree
    delNode();
    // delete indegree
    // delNode(indegree, false);
#ifdef TEST
    auto endTime = system_clock::now();
    duration = duration_cast<microseconds>(endTime - startTime);
    cout <<  "delete Node cost: " << double(duration.count()) * microseconds::period::num / microseconds::period::den << " s." << endl;
#endif

#ifdef TEST
    startTime = system_clock::now();
#endif

    for(int i=0; i<nodeNum; ++i)
        if(next_num[i] != 0)
        {
            sort(Graph[i].nexts, Graph[i].nexts+next_num[i], cmp);
            sort(Graph[i].prevs, Graph[i].prevs+prev_num[i], cmp);
        }

#ifdef TEST
    auto buildEnd = system_clock::now();
    endTime = system_clock::now();
    duration = duration_cast<microseconds>(endTime - startTime);
    cout <<  "Sort nexts and prevs cost: " << double(duration.count()) * microseconds::period::num / microseconds::period::den << " s." << endl;
    duration = duration_cast<microseconds>(buildEnd - buildStart);
    cout << "build graph cost: "<< double(duration.count()) * microseconds::period::num / microseconds::period::den << " s." << endl;
    startTime = system_clock::now();
#endif

    multi_findCyle();

#ifdef TEST
    endTime = system_clock::now();
    duration = duration_cast<microseconds>(endTime - startTime);
    cout <<  "find cycle cost: " << double(duration.count()) * microseconds::period::num / microseconds::period::den << " s." << endl;
    startTime = system_clock::now();
#endif

    save_results(outputFile);

#ifdef TEST
    endTime = system_clock::now();
    duration = duration_cast<microseconds>(endTime - startTime);
    cout <<  "Write data cost: " << double(duration.count()) * microseconds::period::num / microseconds::period::den << " s." << endl;
#endif
    return 0;
}
