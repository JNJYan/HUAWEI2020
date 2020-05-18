#include <fcntl.h>
#include <sys/mman.h>
#include <sys/time.h>
#include <unistd.h>
#include <sys/types.h>
 #include <sys/stat.h>
#include <algorithm>
#include <atomic>
#include <cmath>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <numeric>
#include <string>
#include <thread>
#include <vector>
using namespace std;
using namespace chrono;
#define ThreadNum 4

#define  likely(x)     __builtin_expect(!!(x), 1)
#define  unlikely(x)   __builtin_expect(!!(x), 0)

#define TEST
#ifdef TEST
string testFile = "./data/test_data4.txt";
// string testFile = "./data/351244.txt";
string outputFile = "./projects/student/result.txt";
#else
string testFile = "/data/test_data.txt";
string outputFile = "/projects/student/result.txt";
#endif

const int EDGES = 3000000 + 7;
const int NODES = EDGES * 2;


int nodeNum = 0;
int ansNum = 0;  


struct Findtool {
    int reach_cnt = 0;
    char path[NODES]; 
    int reach[NODES];
    int64_t end_money[NODES];
} tools[ThreadNum];

int edge_cnt = 0; 
int valid_cnt = 0;   
int ansnum[ThreadNum][6];

vector<int> ans[6][NODES];

struct Id2char{
    int len;
    char str[12];
};

Id2char idmid[NODES];

int inputs[NODES];
int64_t money[NODES];
int isNode[NODES];                                    // 有效点

struct Edge {
    int v;
    int64_t w;
    // double down;
    // double up;
    Edge(){};
    Edge(int v, int64_t w): v(v), w(w){
    //     down = 0.2 * w;
    //     up = 3 * w;
    };
    bool operator<(const Edge& b)const{
        return v < b.v;
    };
};

bool cmp(const Edge& a, const Edge& b)
{
    return a.v > b.v;
}

Edge *Nexts[NODES];
Edge *Prevs[NODES];


int next_num[NODES], prev_num[NODES];
atomic<int> indegree[NODES], outdegree[NODES];
atomic<int> next_num_atomic[NODES], prev_num_atomic[NODES];

Edge Next[EDGES];
Edge Prev[EDGES];

const char table[201] = "00010203040506070809101112131415161718192021222324252627282930313233343536373839404142434445464748495051525354555657585960616263646566676869707172737475767778798081828384858687888990919293949596979899";
void itoa(const int &i, int x)
{
    auto &pidmid = idmid[i];
    char aa[20];
    char *cur = pidmid.str;
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
    if(x < 10)
        aa[len++] = table[2*x + 1];
    for(int j=len-1;j>=0;j--)
        *cur++ = aa[j];
    *cur++ = ',';
    pidmid.len = len+1;
}


int valid_cur = 0;
atomic_flag lock = ATOMIC_FLAG_INIT;
inline void GetId(int & cur) {
    while(lock.test_and_set());
    if(valid_cur < valid_cnt)
        cur = isNode[valid_cur++];
    else
        cur = -1;
    lock.clear();
}


struct Hash{
    int key;
    int value;
    Hash (): key(-1), value(0) {}
};

const int hash_gap = EDGES / 7;
Hash appears[NODES];
Hash idHash[NODES];

void Hash_Add(const int & num, int & i)
{
    int index = num % NODES;
    while (idHash[index].key != -1) {
        index = (index + hash_gap) % NODES;
    }
    idHash[index].key = num;
    idHash[index].value = i;
}


void Degree(const int & start, const int & end) {
    int u, v;
    for(int i = start; i < end; i += 2) {
        u = inputs[i];
        v = inputs[i+1];
        ++outdegree[u];
        ++indegree[v];
    }
}

int appear[4000000];
int MapInt2Str(int * inputs, int n) {
    int index;
    int j = 0;

    for(int i = 0; i < n; ++i) {
        index = inputs[i] % NODES;
        while (appears[index].key != -1 && appears[index].key != inputs[i]) {
            index = (index + hash_gap) % (NODES);
        }
        if(appears[index].key == -1) {
            appears[index].key = inputs[i];
            appear[j++] = inputs[i];
        }
    }
    sort(appear, appear+j);

    int * p = appear;
    for(int i = 0; i < j; ++i) {
        itoa(i, *p);
        Hash_Add(*p, i);
        p++;
    }
    for(int i = 0; i < n; ++i) {
        index = inputs[i] % NODES;
        while (idHash[index].key != inputs[i]){
            index = (index + hash_gap) % NODES;
        }
        inputs[i] = idHash[index].value;
    }
    return j;
}

void buildgraph(const int & start, const int & end) {
    Edge *p;
    int u, v;
    int64_t weight;
    int u_index, v_index;
    for(int i = start; i < end; i += 2) {
        u = inputs[i];
        v = inputs[i+1];
        weight = money[i/2];
        u_index = ++next_num_atomic[u];
        p = Nexts[u] + u_index - 1;
        *p = Edge(v, weight);

        v_index = ++prev_num_atomic[v];
        p = Prevs[v] + v_index - 1;
        *p = Edge(u, weight);
    }
}
int inputs_cnt = 0;
void readData()
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
    int64_t weight;
    int edge_cnt = 0;
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
        money[edge_cnt++] = weight;
    }while(*buf != '\0');
#ifdef TEST
    auto t2 = system_clock::now();
    auto duration = duration_cast<microseconds>(t2 - t1);
    cout <<  "Read data cost: " << double(duration.count()) * microseconds::period::num / microseconds::period::den << " s." << endl;
#endif

}
void buildGraph(){
#ifdef TEST
    auto t2 = system_clock::now();
#endif    
    nodeNum = MapInt2Str(inputs, inputs_cnt);

#ifdef TEST
    auto t3 = system_clock::now();
    auto duration = duration_cast<microseconds>(t3 - t2);
    cout <<  "Hash map cost: " << double(duration.count()) * microseconds::period::num / microseconds::period::den << " s." << endl;
#endif

    int edge_num = edge_cnt / 3;
    int per_num = inputs_cnt / ThreadNum;
    thread ths[ThreadNum];
    for(int i = 0; i < ThreadNum; ++i) {
        ths[i] = thread(Degree, i * per_num, (i + 1) * per_num);
    }
    for(int i = 0; i < ThreadNum; ++i) {
        ths[i].join();
    }

#ifdef TEST
    auto t4 = system_clock::now();
    duration = duration_cast<microseconds>(t4 - t3);
    cout <<  "Cal degree cost: " << double(duration.count()) * microseconds::period::num / microseconds::period::den << " s." << endl;
#endif

    Edge *pnext = Next, *pprev = Prev;
    for(int i = 0; i < nodeNum; i++) {
        Nexts[i] = pnext;
        Prevs[i] = pprev;
        pnext += outdegree[i];
        pprev += indegree[i];
    }

    for(int i = 0; i < ThreadNum; ++i)
        ths[i] = thread(buildgraph, i*per_num, (i+1)*per_num);
    for(int i = 0; i < ThreadNum; ++i)
        ths[i].join();

#ifdef TEST
    auto t5 = system_clock::now();
    duration = duration_cast<microseconds>(t5 - t4);
    cout <<  "Reindex cost: " << double(duration.count()) * microseconds::period::num / microseconds::period::den << " s." << endl;
#endif

    memcpy(next_num, next_num_atomic, nodeNum*sizeof(int));
    memcpy(prev_num, prev_num_atomic, nodeNum*sizeof(int));

#ifdef TEST
    auto t6 = system_clock::now();
    duration = duration_cast<microseconds>(t6 - t5);
    cout <<  "Build Graph cost: " << double(duration.count()) * microseconds::period::num / microseconds::period::den << " s." << endl;
#endif

    Edge * g = Next, * r = Prev;
    for(int i = 0; i < nodeNum; ++i) {
        sort(g, g + next_num[i]);
        sort(r, r + prev_num[i], cmp);
        g += outdegree[i];
        r += indegree[i];
    }

#ifdef TEST
    auto t7 = system_clock::now();
    duration = duration_cast<microseconds>(t7 - t6);
    cout <<  "Sort Graph cost: " << double(duration.count()) * microseconds::period::num / microseconds::period::den << " s." << endl;
#endif

    for(int i=0; i<nodeNum; ++i) {
        if(indegree[i] !=0 && outdegree[i] != 0)
            isNode[valid_cnt++] = i;
    }

#ifdef TEST
    auto t8 = system_clock::now();
    duration = duration_cast<microseconds>(t8 - t7);
    cout <<  "Get valid ID cost: " << double(duration.count()) * microseconds::period::num / microseconds::period::den << " s." << endl;
#endif
}



inline bool check(const int64_t &x, const int64_t &y) {
    return  x <= 5*y && y <= 3*x;
}

// inline bool check(const Edge & x, const Edge & y) {
//     return y.w <= x.up && x.down <= y.w;
// }

inline bool check(const Edge &x, const Edge &y) {
    return  x.w <= 5*y.w && y.w <= 3*x.w;
}

void DFS(Findtool &tool, const int &head, const int &tid) {

    for(int i = 0; i< tool.reach_cnt; ++i) {
        tool.path[tool.reach[i]] = 0;
    }
    tool.path[head] = 15;
    tool.reach_cnt = 0;
    tool.reach[tool.reach_cnt++] = head;
    Edge *prev1, *prev2, *prev3, *prev4;
    prev1 = Prevs[head];
    for(int i1 = 0; i1 < prev_num[head]; ++i1, ++prev1) {
        const int & v1 = prev1->v;
        if(v1 <= head)
            break;
        const int64_t &w1 = prev1->w;
        tool.end_money[v1] = w1;
        tool.path[v1] = 15;
        tool.reach[tool.reach_cnt++] = v1;
        prev2 = Prevs[v1];
        for(int i2 = 0; i2 < prev_num[v1]; ++i2, ++prev2) {
            const int & v2 = prev2->v;
            if(v2 <= head)
                break;
            const int64_t &w2 = prev2->w;
            if(!check(w2, w1))
                continue;
            tool.path[v2] |= 14;
            tool.reach[tool.reach_cnt++] = v2;
            prev3 = Prevs[v2];
            for(int i3 = 0; i3 < prev_num[v2]; ++i3, ++prev3) {
                const int &v3 = prev3->v;
                if(v3 <= head)
                    break;
                const int64_t &w3 = prev3->w;
                if(!check(w3, w2))
                    continue;
                tool.path[v3] |= 12;
                tool.reach[tool.reach_cnt++] = v3;
                prev4 = Prevs[v3];
                for(int i4 = 0; i4 < prev_num[v3]; ++i4, ++prev4) {
                    const int &v4 = prev4->v;
                    if(v4 <= head)
                        break;
                    const int64_t &w4 = prev4->w;
                    if(!check(w4, w3))
                        continue;
                    tool.path[v4] |= 8;
                    tool.reach[tool.reach_cnt++] = v4;
                }
            }
        }
    }

    Edge *next1, *next2, *next3, *next4, *next5, *next6, *next7;
    next1 = Nexts[head];
    int sz1, sz2, sz3, sz4, sz5, sz6, sz7;
    int next1_val, next2_val, next3_val, next4_val, next5_val, next6_val, next7_val;
    sz1 = next_num[head];
    for(int i1=0; i1<sz1; ++i1, ++next1) {
        const auto &e1 = *next1;
        next1_val = e1.v;
        if(next1_val < head)
            continue;
        next2 = Nexts[next1_val];
        sz2 = next_num[next1_val];
        for(int i2=0; i2<sz2; ++i2, ++next2) {
            const auto &e2 = *next2;
            next2_val = e2.v;
            if(next2_val <= head || !check(e1, e2))
                continue;
            next3 = Nexts[next2_val];
            sz3 = next_num[next2_val];
            for(int i3=0; i3<sz3; ++i3, ++next3) {
                const auto &e3 = *next3;
                next3_val = e3.v;
                if(next3_val<head || next3_val == next1_val || !check(e2, e3))
                    continue;
                else if(next3_val == head){
                    if(!check(e3, e1))
                        continue;
                    ans[0][head].emplace_back(head);
                    ans[0][head].emplace_back(next1_val);
                    ans[0][head].emplace_back(next2_val);
                    ++ansnum[tid][2];
                    continue;
                }
                next4 = Nexts[next3_val];
                sz4 = next_num[next3_val];
                for(int i4 = 0; i4 < sz4; ++i4, ++next4) {
                    const auto & e4 = *next4;
                    next4_val = e4.v;
                    if(!(tool.path[next4_val] & 8) || !check(e3, e4))
                        continue;
                    else if(next4_val == head) {
                        if(!check(e4, e1))
                            continue;
                        ans[1][head].emplace_back(head);
                        ans[1][head].emplace_back(next1_val);
                        ans[1][head].emplace_back(next2_val);
                        ans[1][head].emplace_back(next3_val);
                        ++ansnum[tid][1];
                        continue;
                    } else if(next1_val == next4_val || next2_val == next4_val) {
                        continue;
                    }
                    next5 = Nexts[next4_val];
                    sz5 = next_num[next4_val];
                    for(int i5 = 0; i5 < sz5; ++i5, ++next5) {
                        const auto &e5 = *next5;
                        next5_val = e5.v;
                        if(!(tool.path[next5_val] & 4) || !check(e4, e5))
                            continue;
                        else if(next5_val == head)
                        {
                            if(!check(e5, e1))
                                continue;
                            ans[2][head].emplace_back(head);
                            ans[2][head].emplace_back(next1_val);
                            ans[2][head].emplace_back(next2_val);
                            ans[2][head].emplace_back(next3_val);
                            ans[2][head].emplace_back(next4_val);
                            ++ansnum[tid][2];
                            continue;
                        } else if(next1_val == next5_val || next2_val == next5_val || next3_val == next5_val)
                            continue;
                        next6 = Nexts[next5_val];
                        sz6 = next_num[next5_val];
                        for(int i6 = 0; i6 < sz6; ++i6, ++next6) {
                            const auto & e6 = *next6;
                            next6_val = e6.v;
                            if(!(tool.path[next6_val] & 2) || !check(e5, e6))
                                continue;
                            else if(next6_val == head) {
                                if(!check(e6, e1))
                                    continue;
                                ans[3][head].emplace_back(head);
                                ans[3][head].emplace_back(next1_val);
                                ans[3][head].emplace_back(next2_val);
                                ans[3][head].emplace_back(next3_val);
                                ans[3][head].emplace_back(next4_val);
                                ans[3][head].emplace_back(next5_val);
                                ++ansnum[tid][3];
                                continue;
                            }else if(next1_val == next6_val || next2_val == next6_val || next3_val == next6_val || next4_val == next6_val)
                                continue;
                            next7 = Nexts[next6_val];
                            sz7 = next_num[next6_val];
                            for(int i7 = 0; i7 < sz7; ++i7, ++next7)
                            {
                                const auto & e7 = *next7;
                                next7_val = e7.v;
                                if(!(tool.path[next7_val] & 1) || !check(e6, e7))
                                    continue;
                                if(next7_val == head){
                                    if(!check(e7, e1))
                                        continue;
                                    ans[4][head].emplace_back(head);
                                    ans[4][head].emplace_back(next1_val);
                                    ans[4][head].emplace_back(next2_val);
                                    ans[4][head].emplace_back(next3_val);
                                    ans[4][head].emplace_back(next4_val);
                                    ans[4][head].emplace_back(next5_val);
                                    ans[4][head].emplace_back(next6_val);
                                    ++ansnum[tid][4];
                                    continue;
                                }
                                const int64_t &w8 = tool.end_money[next7_val];
                                if(next1_val == next7_val || next2_val == next7_val || next3_val == next7_val || 
                                        next4_val == next7_val || next5_val == next7_val || !check(e7.w, w8) || !check(w8, e1.w))
                                    continue;
                                // cout << "!!!" << endl;
                                ans[5][head].emplace_back(head);
                                ans[5][head].emplace_back(next1_val);
                                ans[5][head].emplace_back(next2_val);
                                ans[5][head].emplace_back(next3_val);
                                ans[5][head].emplace_back(next4_val);
                                ans[5][head].emplace_back(next5_val);
                                ans[5][head].emplace_back(next6_val);
                                ans[5][head].emplace_back(next7_val);
                                ++ansnum[tid][5];
                            }
                        }
                    }
                }
            }
        }
    }
}


void FindCycle(int tid) {
#ifdef TEST
    auto t1 = system_clock::now();
#endif

    int id = 0;
    while (true){
        GetId(id);
        if(id == -1) break;
        DFS(tools[tid], id, tid);
    }

#ifdef TEST
    auto t2 = system_clock::now();
    auto duration = duration_cast<microseconds>(t2 - t1);
    printf("Thread [%d] find cycle cost: %.4f s\n", tid, double(duration.count()) * microseconds::period::num / microseconds::period::den);
#endif
}

void multi_findCyle() {
#ifdef TEST
    auto t1 = system_clock::now();
#endif
    valid_cur = 0;
    thread Th[ThreadNum];
    for(int i = 1; i < ThreadNum; ++i) 
        Th[i] = thread(FindCycle, i);
    FindCycle(0);
    for(int i = 1; i < ThreadNum; ++i) 
        Th[i].join();
#ifdef TEST
    auto t2 = system_clock::now();
    auto duration = duration_cast<microseconds>(t2 - t1);
    cout <<  "Find cycle cost: " << double(duration.count()) * microseconds::period::num / microseconds::period::den << " s." << endl;
#endif
}

void save_results() {
#ifdef TEST
    auto t1 = system_clock::now();
#endif
    int buf_size = 200000;
    char *res = new char[buf_size];
    char *cur = res;
    char *str;
    int len, write_num=0;
    int fp = open(outputFile.c_str(), O_CREAT|O_RDWR|O_TRUNC, 0666);
    for(int tid=0; tid<ThreadNum; ++tid)
        for(int j=0; j<6; ++j)
            ansNum += ansnum[tid][j];
    str = (char*)(to_string(ansNum) + "\n").c_str();
    len = strlen(str);
    memcpy(cur, str, len);
    cur += len;
    int depth;
    for(int i=0; i<6; i++)
    {
        depth = i+3; 
        int *p = isNode;
        for(int j=0; j<valid_cnt; ++j, ++p)
        {
            const int n = ans[i][*p].size();
            auto &tmp = ans[i][*p];
            for(int i=0; i<n;)
            {
                ++write_num;
                for(int k=0; k<depth; ++k, ++i)
                {
                    str = (char*)idmid[tmp[i]].str;
                    len = idmid[tmp[i]].len;
                    memcpy(cur, str, len);
                    cur += len;
                }
                *(cur-1) = '\n';
                if(write_num == 2000)
                {
                    write(fp, res, cur-res);
                    cur = res;
                    write_num = 0;
                }
            }
        }
    }
    write(fp, res, cur-res);
    close(fp);
    delete[] res;
#ifdef TEST
    auto t2 = system_clock::now();
    auto duration = duration_cast<microseconds>(t2 - t1);
    cout <<  "Save Results cost: " << double(duration.count()) * microseconds::period::num / microseconds::period::den << " s." << endl;
#endif
}

int main() {
#ifdef TEST
    auto t1 = system_clock::now();
#endif
    readData();
#ifdef TEST
    auto t2 = system_clock::now();
    auto duration = duration_cast<microseconds>(t2 - t1);
    cout <<  "Read Data cost: " << double(duration.count()) * microseconds::period::num / microseconds::period::den << " s." << endl;
#endif
    buildGraph();
#ifdef TEST
    auto t3 = system_clock::now();
    duration = duration_cast<microseconds>(t3 - t2);
    cout <<  "Total Build cost: " << double(duration.count()) * microseconds::period::num / microseconds::period::den << " s." << endl;
#endif
    multi_findCyle();
    save_results();

#ifdef TEST
    auto t4 = system_clock::now();
    duration = duration_cast<microseconds>(t4 - t3);
    cout << "total ans: " << ansNum << endl;
    cout <<  "Whole program cost: " << double(duration.count()) * microseconds::period::num / microseconds::period::den << " s." << endl;
#endif
    return 0;
}