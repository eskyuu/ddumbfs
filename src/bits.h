/* bits.h (c) alain.spineux@gmail.com
 *
 * handle array of bits
 */
#ifndef __BITS_H
#define __BITS_H

#include <stdint.h>
#include <arpa/inet.h>

// bits are stored in bigendian format into memory or when memory is
// mapped from file. Highest bit is the bit 0, lowest one is 31

typedef uint32_t bit_int;
#define BIT_INT_HIGHEST_BIT ((bit_int)0x80000000)
#define BE_TO_BIT_INT(x)    ntohl(x)
#define BIT_INT_TO_BE(x)    htonl(x)
#define BIT_INT_BIT_COUNT	32
#define BIT_INT_BIT_COUNT1	31
#define BIT_INT_BYTE		4
#define BIT_INT_BYTE1		3
#define BIT_INT_SHIFT		5
#define BIT_INT_OFF_MASK	0x1F

struct bit_array
{
    long long int size;   // size in bits
    long long int isize;  // size in bit_int
    long long int index;  // index to help finding a free block
    bit_int mask_last;
    bit_int *array;
    bit_int *last;        // the last bit_int (don't forget to apply the mask)
    bit_int *end;         // past the last bit_int
    int allocated;
};

int bit_array_init(struct bit_array *ba, long long int size, int pattern);
int bit_array_init2(struct bit_array *ba, long long int size, void *buffer);
void bit_array_release(struct bit_array *ba);
void bit_array_reset(struct bit_array *ba, int pattern);
void bit_array_random(struct bit_array *ba);
int bit_array_set(struct bit_array *ba, long long int bit_addr);
int bit_array_unset(struct bit_array *ba, long long int bit_addr);
void bit_array_reset_zone(struct bit_array *ba, long long int from, long long int to, int set);
int bit_array_get(struct bit_array *ba, long long int bit_addr);
void bit_array_print(struct bit_array *ba);
long long int bit_array_search_first_set(struct bit_array *ba, long long int from);
long long int bit_array_search_first_unset(struct bit_array *ba, long long int from);
long long int bit_array_search_nth_set(struct bit_array *ba, long long int from, long long int n);
long long int bit_array_search_nth_unset(struct bit_array *ba, long long int from, long long int n);
long long int bit_array_count_zone(struct bit_array *ba, long long int from, long long int to);

void bit_array_invers_into(struct bit_array *src, struct bit_array *dst);
void bit_array_bwand(struct bit_array *src, struct bit_array *dst);
void bit_array_bwor(struct bit_array *src, struct bit_array *dst);
void bit_array_plus_diff(struct bit_array *a, struct bit_array *b, struct bit_array *c); // a=a+(b-c)
void bit_array_copy(struct bit_array *src, struct bit_array *dst);
int bit_array_cmp(struct bit_array *ba1, struct bit_array *ba2, int *cmp);
int bit_array_cmp_count(struct bit_array *ba1, struct bit_array *ba2, long long int *only1, long long int *only2, long long int *both);
long long int bit_array_alloc(struct bit_array *ba);
void bit_array_count(struct bit_array *ba, long long int *s, long long int *u);

int bit_array_save(struct bit_array *ba, char *filename, int sync);
int bit_array_load(struct bit_array *ba, char *filename);


enum { qba_count_zone, qba_nth_unset };

struct quick_bit_array
{
	struct bit_array *ba;
	long long int *quick;
	long long int size;
	long long int from;
	long long int to;
	long long int max;
	long long int coef;
};

int quick_bit_array_init(struct quick_bit_array *q, struct bit_array *ba, long long int size, long long int from, long long int to, int what);
void quick_bit_array_release(struct quick_bit_array *q);
long long int quick_bit_array_count_zone(struct quick_bit_array *q, long long int bit_addr);
long long int quick_bit_array_search_nth_unset(struct quick_bit_array *q, long long int count);




#endif
