#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <sys/time.h>
#include <pthread.h>


#define NOW_PER_SEC 1000
#define NBR_SEC       10 // run 10 sec each test

long long int now()
{
    struct timeval tv;
    gettimeofday(&tv, NULL);
//    fprintf(stderr, "now=%lld %lld %lld %lld %lld\n", tv.tv_sec*1000LL+tv.tv_usec/1000LL, tv.tv_sec*1000LL, tv.tv_usec/1000LL, (long long int)tv.tv_sec, (long long int)tv.tv_usec);
    return tv.tv_sec*1000LL+tv.tv_usec/1000LL;
}

pthread_mutex_t mutex=PTHREAD_MUTEX_INITIALIZER;
pthread_spinlock_t spinlock;

static void test_spinlock(int n)
{
    while (n>0)
    {
        pthread_spin_lock(&spinlock);
        pthread_spin_unlock(&spinlock);
        n--;
        pthread_spin_lock(&spinlock);
        pthread_spin_unlock(&spinlock);
        n--;
        pthread_spin_lock(&spinlock);
        pthread_spin_unlock(&spinlock);
        n--;
        pthread_spin_lock(&spinlock);
        pthread_spin_unlock(&spinlock);
        n--;
        pthread_spin_lock(&spinlock);
        pthread_spin_unlock(&spinlock);
        n--;
    }
}

static void test_mutex(int n)
{
    while (n>0)
    {
        pthread_mutex_lock(&mutex);
        pthread_mutex_unlock(&mutex);
        n--;
        pthread_mutex_lock(&mutex);
        pthread_mutex_unlock(&mutex);
        n--;
        pthread_mutex_lock(&mutex);
        pthread_mutex_unlock(&mutex);
        n--;
        pthread_mutex_lock(&mutex);
        pthread_mutex_unlock(&mutex);
        n--;
        pthread_mutex_lock(&mutex);
        pthread_mutex_unlock(&mutex);
        n--;
    }
}

static void test_empty(int n)
{
    while (n>0)
    {
        n--;
        n--;
        n--;
        n--;
        n--;
    }
}

void test(char *name, void (*func)(int n), int n)
{
    int i;

    long long int start, end, stop;
    start=now();
    end=start;
    stop=start+NBR_SEC*NOW_PER_SEC;

//    fprintf(stderr, "%lld %lld %lld\n", start, end, stop);
    i=0;
    while (end<stop)
    {
//        fprintf(stderr, "%lld %lld %lld\n", start, end, stop);
        func(n);
        i++;
        end=now();
    }

    fprintf(stderr, "%s   time: %15.1f lock/s\n", name, (float)n*i*NOW_PER_SEC/(end-start));
}

pthread_t loop1_thread, loop2_thread;
pthread_rwlock_t rw_lock=PTHREAD_RWLOCK_INITIALIZER;
pthread_cond_t cond1=PTHREAD_COND_INITIALIZER;
pthread_cond_t cond2=PTHREAD_COND_INITIALIZER;
long long int loop_counter;
int loop_ready;
int loop_quit;
long long int loop_end;

void *loop1(void *ptr)
{
    pthread_mutex_lock(&mutex);
    while (now()<loop_end)
    {
        while (loop_ready) pthread_cond_wait(&cond1, &mutex);
        loop_ready=1;
        pthread_cond_signal(&cond2);
    }
    loop_quit=1;
    pthread_mutex_unlock(&mutex);
    return NULL;
}

void *loop2(void *ptr)
{
    pthread_mutex_lock(&mutex);
    while (!loop_quit)
    {
        while (!loop_ready) pthread_cond_wait(&cond2, &mutex);
        loop_ready=0;
        loop_counter++;
        pthread_cond_signal(&cond1);
    }
    pthread_mutex_unlock(&mutex);
    return NULL;
}


void loop_switch()
{
    loop_counter=0;
    loop_ready=0;
    loop_quit=0;
    long long int start=now();
    loop_end=start+NBR_SEC*NOW_PER_SEC;

    pthread_create(&loop1_thread, NULL, &loop1, NULL);
    pthread_create(&loop2_thread, NULL, &loop2, NULL);

    pthread_join(loop1_thread, NULL);
    pthread_join(loop2_thread, NULL);

    long long int end=now();
    fprintf(stderr, "thread-switch time: %15.1f switch/s\n", (float)loop_counter*NOW_PER_SEC/(end-start));

}

#define MAX_SIZE    16

struct table
{
    int tn;
    long long int tid[MAX_SIZE];
};

void tinit(struct table* t)
{
    t->tn=0;
}

void tadd(struct table* t, long long int id)
{
    t->tid[t->tn]=id;
    t->tn++;
}

void tdel(struct table* t, long long int id)
{
    int i;
    for (i=0; i<t->tn; i++)
    {
        if (t->tid[i]==id)
        {
            t->tn--;
            if (i!=t->tn) t->tid[i]=t->tid[t->tn];
            break;
        }
    }
}



int main(int argc, char *argv[])
{
    int n=1000;
    pthread_spin_init(&spinlock, 0);
    test("spinlock   ", test_spinlock, n);
    test("mutex      ", test_mutex, n);
    test("empty      ", test_empty, n);
    loop_switch();
    return 0;
}
