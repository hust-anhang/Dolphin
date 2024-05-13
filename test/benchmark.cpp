#include "Timer.h"
#include "Tree.h"
#include "zipf.h"

#include <city.h>
#include <stdlib.h>
#include <thread>
#include <time.h>
#include <unistd.h>
#include <vector>
#include <random>

//////////////////// workload parameters /////////////////////

// #define USE_CORO
// #define waiting_test
const int kCoroCnt = 10;

int kReadRatio;
int kThreadCount;
int kNodeCount;
uint64_t kKeySpace = 4 * define::MB;
double kWarmRatio = 0.5;
double zipfan = 0.99;

//////////////////// workload parameters /////////////////////
extern double cache_miss[MAX_APP_THREAD];
extern double cache_hit[MAX_APP_THREAD];
extern uint64_t lock_fail[MAX_APP_THREAD];
extern uint64_t write_handover_num[MAX_APP_THREAD];
extern uint64_t try_write_op[MAX_APP_THREAD];
extern uint64_t read_handover_num[MAX_APP_THREAD];
extern uint64_t try_read_op[MAX_APP_THREAD];
extern uint64_t read_leaf_retry[MAX_APP_THREAD];
extern uint64_t leaf_cache_invalid[MAX_APP_THREAD];
extern uint64_t try_read_leaf[MAX_APP_THREAD];
extern uint64_t read_node_repair[MAX_APP_THREAD];
extern uint64_t try_read_node[MAX_APP_THREAD];
extern uint64_t read_node_type[MAX_APP_THREAD][MAX_NODE_TYPE_NUM];
extern uint64_t retry_cnt[MAX_APP_THREAD][MAX_FLAG_NUM];

extern double cache_miss[MAX_APP_THREAD];
extern double cache_hit[MAX_APP_THREAD];

extern uint64_t entry_num_in_inner_node[MAX_APP_THREAD][256];
extern uint64_t entry_num_in_leaf_node[MAX_APP_THREAD][kLeafCardinality];

std::thread th[MAX_APP_THREAD];
uint64_t tp[MAX_APP_THREAD][8];

extern uint64_t latency[MAX_APP_THREAD][LATENCY_WINDOWS];
uint64_t latency_th_all[LATENCY_WINDOWS];

Tree *tree;
DSM *dsm;

class RequsetGenBench : public RequstGen {

public:
  RequsetGenBench(int coro_id, DSM *dsm, int id)
      : coro_id(coro_id), dsm(dsm), id(id) {
    seed = rdtsc();
    mehcached_zipf_init(&state, kKeySpace, zipfan,
                        (rdtsc() & (0x0000ffffffffffffull)) ^ id);
  }

  Request next() override {
    Request r;
    uint64_t dis = mehcached_zipf_next(&state);

    r.k = int2key(dis);
    r.v = 23;
    r.is_search = rand_r(&seed) % 100 < kReadRatio;

    tp[id][0]++;

    return r;
  }

private:
  int coro_id;
  DSM *dsm;
  int id;

  unsigned int seed;
  struct zipf_gen_state state;
};

RequstGen *coro_func(int coro_id, DSM *dsm, int id) {
  return new RequsetGenBench(coro_id, dsm, id);
}

Timer bench_timer;
std::atomic<int64_t> warmup_cnt{0};
std::atomic_bool ready{false};
void thread_run(int id) {

  bindCore(id);

  dsm->registerThread();

  uint64_t all_thread = kThreadCount * dsm->getClusterSize();
  uint64_t my_id = kThreadCount * dsm->getMyNodeID() + id;

  printf("I am thread %ld on compute nodes\n", my_id);

  if (id == 0) {
    bench_timer.begin();
  }

  uint64_t end_warm_key = kWarmRatio * kKeySpace;
  for (uint64_t i = 1; i < end_warm_key; ++i) {
    if (i % all_thread == my_id) {
      tree->insert(int2key(i), i);
    }
  }

  warmup_cnt.fetch_add(1);

  if (id == 0) {
    while (warmup_cnt.load() != kThreadCount)
      ;
    printf("node %d finish\n", dsm->getMyNodeID());
    dsm->barrier("warm_finish");

    uint64_t ns = bench_timer.end();
    printf("warmup time %lds\n", ns / 1000 / 1000 / 1000);

    // for (int i = 0; i <= kLeafCardinality; i++) {
    //   entry_num_in_leaf_node[dsm->getMyThreadID()][i] = 0;
    // }
    // for (int i = 0; i <= 256; i++) {
    //   entry_num_in_inner_node[dsm->getMyThreadID()][i] = 0;
    // }
    tree->clear_debug_info();

    ready = true;

    warmup_cnt.store(0);
  }

  while (warmup_cnt.load() != 0)
    ;

#ifdef USE_CORO
  tree->run_coroutine(coro_func, id, kCoroCnt);
#else

  /// without coro
  unsigned int seed = rdtsc();
  struct zipf_gen_state state;
  mehcached_zipf_init(&state, kKeySpace, zipfan,
                      (rdtsc() & (0x0000ffffffffffffull)) ^ id);

  std::random_device rd;
  std::mt19937 gen(rd());   
  std::uniform_int_distribution<int> distribution(1, 1000); // create value

  Timer timer;
  while (true) {

    uint64_t dis = mehcached_zipf_next(&state);
    dis = dis == 0? 1: dis;
    Key key = int2key(dis);

    Value v;

    timer.begin();

    if (rand_r(&seed) % 100 < kReadRatio) { // GET
      tree->search(key, v);
    } else {
      v = distribution(gen);
      tree->insert(key, v);
    }

    auto us_10 = timer.end() / 100;
    if (us_10 >= LATENCY_WINDOWS) {
      us_10 = LATENCY_WINDOWS - 1;
    }
    latency[id][us_10]++;

    tp[id][0]++;
  }

  // for (uint64_t i = 100 * define::MB; i < 1024 * define::MB; ++i) {

  //   if (i % all_thread == my_id) {
  //     timer.begin();
  //     tree->insert(int2key(i), i * 2);
  //     auto us_10 = timer.end() / 100;
  //     if (us_10 >= LATENCY_WINDOWS) {
  //       us_10 = LATENCY_WINDOWS - 1;
  //     }
  //     latency[id][us_10]++;

  //     tp[id][0]++;
  //   }
  // }

#endif

}

void parse_args(int argc, char *argv[]) {
  if (argc != 4) {
    printf("Usage: ./benchmark kNodeCount kReadRatio kThreadCount\n");
    exit(-1);
  }

  kNodeCount = atoi(argv[1]);
  kReadRatio = atoi(argv[2]);
  kThreadCount = atoi(argv[3]);

  printf("kNodeCount %d, kReadRatio %d, kThreadCount %d\n", kNodeCount,
         kReadRatio, kThreadCount);
}

void cal_latency() {
  uint64_t all_lat = 0;
  for (int i = 0; i < LATENCY_WINDOWS; ++i) {
    latency_th_all[i] = 0;
    for (int k = 0; k < MAX_APP_THREAD; ++k) {
      latency_th_all[i] += latency[k][i];
    }
    all_lat += latency_th_all[i];
  }
  uint64_t th10 = all_lat * 1 / 10;
  uint64_t th20 = all_lat * 2 / 10;
  uint64_t th30 = all_lat * 3 / 10;
  uint64_t th40 = all_lat * 4 / 10;
  uint64_t th50 = all_lat / 2;
  uint64_t th60 = all_lat * 6 / 10;
  uint64_t th70 = all_lat * 7 / 10;
  uint64_t th80 = all_lat * 8 / 10;
  uint64_t th90 = all_lat * 9 / 10;
  uint64_t th95 = all_lat * 95 / 100;
  uint64_t th99 = all_lat * 99 / 100;
  uint64_t th999 = all_lat * 999 / 1000;

  uint64_t cum = 0;
  for (int i = 0; i < LATENCY_WINDOWS; ++i) {
    cum += latency_th_all[i];
    if (cum >= th10) {
      printf("p10 %.1f\t", i / 10.0);
      th10 = -1;
    }
    if (cum >= th20) {
      printf("p20 %.1f\t", i / 10.0);
      th20 = -1;
    }
    if (cum >= th30) {
      printf("p30 %.1f\t", i / 10.0);
      th30 = -1;
    }
    if (cum >= th40) {
      printf("p40 %.1f\t", i / 10.0);
      th40 = -1;
    }
    if (cum >= th50) {
      printf("p50 %.1f\t", i / 10.0);
      th50 = -1;
    }
    if (cum >= th60) {
      printf("p60 %.1f\t", i / 10.0);
      th60 = -1;
    }
    if (cum >= th70) {
      printf("p70 %.1f\t", i / 10.0);
      th70 = -1;
    }
    if (cum >= th80) {
      printf("p80 %.1f\t", i / 10.0);
      th80 = -1;
    }
    if (cum >= th90) {
      printf("p90 %.1f\t", i / 10.0);
      th90 = -1;
    }
    if (cum >= th95) {
      printf("p95 %.1f\t", i / 10.0);
      th95 = -1;
    }
    if (cum >= th99) {
      printf("p99 %.1f\t", i / 10.0);
      th99 = -1;
    }
    if (cum >= th999) {
      printf("p999 %.1f\n", i / 10.0);
      th999 = -1;
      return;
    }
  }
}

void print_detail() {
    uint64_t try_write_op_cnt = 0, write_handover_cnt = 0;
    for (int i = 0; i < MAX_APP_THREAD; ++i) {
      write_handover_cnt += write_handover_num[i];
      try_write_op_cnt += try_write_op[i];
    }

    uint64_t try_read_op_cnt = 0, read_handover_cnt = 0;
    for (int i = 0; i < MAX_APP_THREAD; ++i) {
      read_handover_cnt += read_handover_num[i];
      try_read_op_cnt += try_read_op[i];
    }

    uint64_t try_read_leaf_cnt = 0, read_leaf_retry_cnt = 0, leaf_cache_invalid_cnt = 0;
    for (int i = 0; i < MAX_APP_THREAD; ++i) {
      try_read_leaf_cnt += try_read_leaf[i];
      read_leaf_retry_cnt += read_leaf_retry[i];
      leaf_cache_invalid_cnt += leaf_cache_invalid[i];
    }

    uint64_t try_read_node_cnt = 0, read_node_repair_cnt = 0;
    for (int i = 0; i < MAX_APP_THREAD; ++i) {
      try_read_node_cnt += try_read_node[i];
      read_node_repair_cnt += read_node_repair[i];
    }

    uint64_t read_node_type_cnt[MAX_NODE_TYPE_NUM];
    memset(read_node_type_cnt, 0, sizeof(uint64_t) * MAX_NODE_TYPE_NUM);
    for (int i = 0; i < MAX_NODE_TYPE_NUM; ++i) {
      for (int j = 0; j < MAX_APP_THREAD; ++j) {
        read_node_type_cnt[i] += read_node_type[j][i];
      }
    }

    uint64_t all_retry_cnt[MAX_FLAG_NUM];
    memset(all_retry_cnt, 0, sizeof(uint64_t) * MAX_FLAG_NUM);
    for (int i = 0; i < MAX_FLAG_NUM; ++i) {
      for (int j = 0; j < MAX_APP_THREAD; ++j) {
        all_retry_cnt[i] += retry_cnt[j][i];
      }
    }
    tree->clear_debug_info();

    printf("write combining rate: %lf\n", write_handover_cnt * 1.0 / try_write_op_cnt);
    printf("read delegation rate: %lf\n", read_handover_cnt * 1.0 / try_read_op_cnt);
    printf("read leaf retry rate: %lf\n", read_leaf_retry_cnt * 1.0 / try_read_leaf_cnt);
    printf("read invalid leaf rate: %lf\n", leaf_cache_invalid_cnt * 1.0 / try_read_leaf_cnt);
    printf("read node repair rate: %lf\n", read_node_repair_cnt * 1.0 / try_read_node_cnt);
    printf("read invalid node rate: %lf\n", all_retry_cnt[INVALID_NODE] * 1.0 / try_read_node_cnt);
    for (int i = 1; i < MAX_NODE_TYPE_NUM; ++ i) {
      printf("node_type%d %lu   ", i, read_node_type_cnt[i]);
    }
    printf("\n\n");
}

int main(int argc, char *argv[]) {

  parse_args(argc, argv);

  DSMConfig config;
  config.machineNR = kNodeCount;
  dsm = DSM::getInstance(config);

  dsm->registerThread();
  tree = new Tree(dsm);

  if (dsm->getMyNodeID() == 0) {
    for (uint64_t i = 1; i < 16000; ++i) {
      tree->insert(int2key(i), i * 2);
    }
  }

  dsm->barrier("benchmark");
  dsm->resetThread();

  for (int i = 0; i < kThreadCount; i++) {
    th[i] = std::thread(thread_run, i);
  }

  while (!ready.load())
    ;

  timespec s, e;
  uint64_t pre_tp = 0;

  int count = 0;

  clock_gettime(CLOCK_REALTIME, &s);
  while (true) {

    sleep(2);
    clock_gettime(CLOCK_REALTIME, &e);
    int microseconds = (e.tv_sec - s.tv_sec) * 1000000 +
                       (double)(e.tv_nsec - s.tv_nsec) / 1000;

    uint64_t all_tp = 0;
    for (int i = 0; i < kThreadCount; ++i) {
      all_tp += tp[i][0];
    }
    uint64_t cap = all_tp - pre_tp;
    pre_tp = all_tp;

    double all = 0, hit = 0;
    for (int i = 0; i < MAX_APP_THREAD; ++i) {
      all += (cache_hit[i] + cache_miss[i]);
      hit += cache_hit[i];
    }

    clock_gettime(CLOCK_REALTIME, &s);

    if (++count % 3 == 0) {
      cal_latency();
    }

    double per_node_tp = cap * 1.0 / microseconds;
    uint64_t cluster_tp = dsm->sum((uint64_t)(per_node_tp * 1000));

    printf("%d, throughput %.4f\n", dsm->getMyNodeID(), per_node_tp);

    printf("cluster throughput %.3f\n", cluster_tp / 1000.0);
    printf("cache hit rate: %lf\n", hit * 1.0 / all);

    // if (count % 3 == 0) {
    //   print_detail();
    // }


    // for (int i = 1; i <= kLeafCardinality; i++) {
    //   printf("%d-%lu ", i, entry_num_in_leaf_node[dsm->getMyThreadID()][i]);
    // }
    // printf("\n\n");
    // for (int i = 1; i <= 32; i++) {
    //   printf("%d-%lu ", i, entry_num_in_inner_node[dsm->getMyThreadID()][i]);
    // }
    // printf("\n");
  }

  return 0;
}
