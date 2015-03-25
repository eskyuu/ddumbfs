/* bits.c (c) alain.spineux@gmail.com
 *
 * handle array of bits
 */

#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <errno.h>
#include <time.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <unistd.h>
#include <fcntl.h>

#include "bits.h"

bit_int bit_int_zero=0;
bit_int bit_int_inv=~(bit_int)0;

// number of bit ON in a byte
signed char bit_count[256]= { 0, 1, 1, 2, 1, 2, 2, 3, 1, 2, 2, 3, 2, 3, 3, 4, 1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5, 1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5, 2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6, 1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5, 2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6, 2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6, 3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7, 1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5, 2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6, 2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6, 3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7, 2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6, 3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7, 3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7, 4, 5, 5, 6, 5, 6, 6, 7, 5, 6, 6, 7, 6, 7, 7, 8, };
// This is 8 tables that holds the place of the Nth bit at ON in a byte,
// 0 means the first one (or the highest bit),
// 8 means the byte don't have enough bits at ON
signed char bit_pos1[256]= { 8, 7, 6, 6, 5, 5, 5, 5, 4, 4, 4, 4, 4, 4, 4, 4, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, };
signed char bit_pos2[256]= { 8, 8, 8, 7, 8, 7, 6, 6, 8, 7, 6, 6, 5, 5, 5, 5, 8, 7, 6, 6, 5, 5, 5, 5, 4, 4, 4, 4, 4, 4, 4, 4, 8, 7, 6, 6, 5, 5, 5, 5, 4, 4, 4, 4, 4, 4, 4, 4, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 8, 7, 6, 6, 5, 5, 5, 5, 4, 4, 4, 4, 4, 4, 4, 4, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 8, 7, 6, 6, 5, 5, 5, 5, 4, 4, 4, 4, 4, 4, 4, 4, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, };
signed char bit_pos3[256]= { 8, 8, 8, 8, 8, 8, 8, 7, 8, 8, 8, 7, 8, 7, 6, 6, 8, 8, 8, 7, 8, 7, 6, 6, 8, 7, 6, 6, 5, 5, 5, 5, 8, 8, 8, 7, 8, 7, 6, 6, 8, 7, 6, 6, 5, 5, 5, 5, 8, 7, 6, 6, 5, 5, 5, 5, 4, 4, 4, 4, 4, 4, 4, 4, 8, 8, 8, 7, 8, 7, 6, 6, 8, 7, 6, 6, 5, 5, 5, 5, 8, 7, 6, 6, 5, 5, 5, 5, 4, 4, 4, 4, 4, 4, 4, 4, 8, 7, 6, 6, 5, 5, 5, 5, 4, 4, 4, 4, 4, 4, 4, 4, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 8, 8, 8, 7, 8, 7, 6, 6, 8, 7, 6, 6, 5, 5, 5, 5, 8, 7, 6, 6, 5, 5, 5, 5, 4, 4, 4, 4, 4, 4, 4, 4, 8, 7, 6, 6, 5, 5, 5, 5, 4, 4, 4, 4, 4, 4, 4, 4, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 8, 7, 6, 6, 5, 5, 5, 5, 4, 4, 4, 4, 4, 4, 4, 4, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, };
signed char bit_pos4[256]= { 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 7, 8, 8, 8, 8, 8, 8, 8, 7, 8, 8, 8, 7, 8, 7, 6, 6, 8, 8, 8, 8, 8, 8, 8, 7, 8, 8, 8, 7, 8, 7, 6, 6, 8, 8, 8, 7, 8, 7, 6, 6, 8, 7, 6, 6, 5, 5, 5, 5, 8, 8, 8, 8, 8, 8, 8, 7, 8, 8, 8, 7, 8, 7, 6, 6, 8, 8, 8, 7, 8, 7, 6, 6, 8, 7, 6, 6, 5, 5, 5, 5, 8, 8, 8, 7, 8, 7, 6, 6, 8, 7, 6, 6, 5, 5, 5, 5, 8, 7, 6, 6, 5, 5, 5, 5, 4, 4, 4, 4, 4, 4, 4, 4, 8, 8, 8, 8, 8, 8, 8, 7, 8, 8, 8, 7, 8, 7, 6, 6, 8, 8, 8, 7, 8, 7, 6, 6, 8, 7, 6, 6, 5, 5, 5, 5, 8, 8, 8, 7, 8, 7, 6, 6, 8, 7, 6, 6, 5, 5, 5, 5, 8, 7, 6, 6, 5, 5, 5, 5, 4, 4, 4, 4, 4, 4, 4, 4, 8, 8, 8, 7, 8, 7, 6, 6, 8, 7, 6, 6, 5, 5, 5, 5, 8, 7, 6, 6, 5, 5, 5, 5, 4, 4, 4, 4, 4, 4, 4, 4, 8, 7, 6, 6, 5, 5, 5, 5, 4, 4, 4, 4, 4, 4, 4, 4, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, };
signed char bit_pos5[256]= { 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 7, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 7, 8, 8, 8, 8, 8, 8, 8, 7, 8, 8, 8, 7, 8, 7, 6, 6, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 7, 8, 8, 8, 8, 8, 8, 8, 7, 8, 8, 8, 7, 8, 7, 6, 6, 8, 8, 8, 8, 8, 8, 8, 7, 8, 8, 8, 7, 8, 7, 6, 6, 8, 8, 8, 7, 8, 7, 6, 6, 8, 7, 6, 6, 5, 5, 5, 5, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 7, 8, 8, 8, 8, 8, 8, 8, 7, 8, 8, 8, 7, 8, 7, 6, 6, 8, 8, 8, 8, 8, 8, 8, 7, 8, 8, 8, 7, 8, 7, 6, 6, 8, 8, 8, 7, 8, 7, 6, 6, 8, 7, 6, 6, 5, 5, 5, 5, 8, 8, 8, 8, 8, 8, 8, 7, 8, 8, 8, 7, 8, 7, 6, 6, 8, 8, 8, 7, 8, 7, 6, 6, 8, 7, 6, 6, 5, 5, 5, 5, 8, 8, 8, 7, 8, 7, 6, 6, 8, 7, 6, 6, 5, 5, 5, 5, 8, 7, 6, 6, 5, 5, 5, 5, 4, 4, 4, 4, 4, 4, 4, 4, };
signed char bit_pos6[256]= { 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 7, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 7, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 7, 8, 8, 8, 8, 8, 8, 8, 7, 8, 8, 8, 7, 8, 7, 6, 6, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 7, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 7, 8, 8, 8, 8, 8, 8, 8, 7, 8, 8, 8, 7, 8, 7, 6, 6, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 7, 8, 8, 8, 8, 8, 8, 8, 7, 8, 8, 8, 7, 8, 7, 6, 6, 8, 8, 8, 8, 8, 8, 8, 7, 8, 8, 8, 7, 8, 7, 6, 6, 8, 8, 8, 7, 8, 7, 6, 6, 8, 7, 6, 6, 5, 5, 5, 5, };
signed char bit_pos7[256]= { 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 7, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 7, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 7, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 7, 8, 8, 8, 8, 8, 8, 8, 7, 8, 8, 8, 7, 8, 7, 6, 6, };
signed char bit_pos8[256]= { 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 7, };
// This is 8 tables that holds the place of the Nth bit at ON in a byte,
// 1 means the first one (or the highest bit),
// O or negative value means not found and contains the number of bit at ON in the bit
// BE CAREFULL THIS IS NOT COMPATIBLE WITH bit_pos above ! bit_poscnt[x]=1+bit_pos[x]
signed char bit_poscnt1[256]= { 0,  8,  7,  7,  6,  6,  6,  6,  5,  5,  5,  5,  5,  5,  5,  5,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1, };
signed char bit_poscnt2[256]= { 0, -1, -1,  8, -1,  8,  7,  7, -1,  8,  7,  7,  6,  6,  6,  6, -1,  8,  7,  7,  6,  6,  6,  6,  5,  5,  5,  5,  5,  5,  5,  5, -1,  8,  7,  7,  6,  6,  6,  6,  5,  5,  5,  5,  5,  5,  5,  5,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4, -1,  8,  7,  7,  6,  6,  6,  6,  5,  5,  5,  5,  5,  5,  5,  5,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3, -1,  8,  7,  7,  6,  6,  6,  6,  5,  5,  5,  5,  5,  5,  5,  5,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2, };
signed char bit_poscnt3[256]= { 0, -1, -1, -2, -1, -2, -2,  8, -1, -2, -2,  8, -2,  8,  7,  7, -1, -2, -2,  8, -2,  8,  7,  7, -2,  8,  7,  7,  6,  6,  6,  6, -1, -2, -2,  8, -2,  8,  7,  7, -2,  8,  7,  7,  6,  6,  6,  6, -2,  8,  7,  7,  6,  6,  6,  6,  5,  5,  5,  5,  5,  5,  5,  5, -1, -2, -2,  8, -2,  8,  7,  7, -2,  8,  7,  7,  6,  6,  6,  6, -2,  8,  7,  7,  6,  6,  6,  6,  5,  5,  5,  5,  5,  5,  5,  5, -2,  8,  7,  7,  6,  6,  6,  6,  5,  5,  5,  5,  5,  5,  5,  5,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4, -1, -2, -2,  8, -2,  8,  7,  7, -2,  8,  7,  7,  6,  6,  6,  6, -2,  8,  7,  7,  6,  6,  6,  6,  5,  5,  5,  5,  5,  5,  5,  5, -2,  8,  7,  7,  6,  6,  6,  6,  5,  5,  5,  5,  5,  5,  5,  5,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4, -2,  8,  7,  7,  6,  6,  6,  6,  5,  5,  5,  5,  5,  5,  5,  5,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3, };
signed char bit_poscnt4[256]= { 0, -1, -1, -2, -1, -2, -2, -3, -1, -2, -2, -3, -2, -3, -3,  8, -1, -2, -2, -3, -2, -3, -3,  8, -2, -3, -3,  8, -3,  8,  7,  7, -1, -2, -2, -3, -2, -3, -3,  8, -2, -3, -3,  8, -3,  8,  7,  7, -2, -3, -3,  8, -3,  8,  7,  7, -3,  8,  7,  7,  6,  6,  6,  6, -1, -2, -2, -3, -2, -3, -3,  8, -2, -3, -3,  8, -3,  8,  7,  7, -2, -3, -3,  8, -3,  8,  7,  7, -3,  8,  7,  7,  6,  6,  6,  6, -2, -3, -3,  8, -3,  8,  7,  7, -3,  8,  7,  7,  6,  6,  6,  6, -3,  8,  7,  7,  6,  6,  6,  6,  5,  5,  5,  5,  5,  5,  5,  5, -1, -2, -2, -3, -2, -3, -3,  8, -2, -3, -3,  8, -3,  8,  7,  7, -2, -3, -3,  8, -3,  8,  7,  7, -3,  8,  7,  7,  6,  6,  6,  6, -2, -3, -3,  8, -3,  8,  7,  7, -3,  8,  7,  7,  6,  6,  6,  6, -3,  8,  7,  7,  6,  6,  6,  6,  5,  5,  5,  5,  5,  5,  5,  5, -2, -3, -3,  8, -3,  8,  7,  7, -3,  8,  7,  7,  6,  6,  6,  6, -3,  8,  7,  7,  6,  6,  6,  6,  5,  5,  5,  5,  5,  5,  5,  5, -3,  8,  7,  7,  6,  6,  6,  6,  5,  5,  5,  5,  5,  5,  5,  5,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4, };
signed char bit_poscnt5[256]= { 0, -1, -1, -2, -1, -2, -2, -3, -1, -2, -2, -3, -2, -3, -3, -4, -1, -2, -2, -3, -2, -3, -3, -4, -2, -3, -3, -4, -3, -4, -4,  8, -1, -2, -2, -3, -2, -3, -3, -4, -2, -3, -3, -4, -3, -4, -4,  8, -2, -3, -3, -4, -3, -4, -4,  8, -3, -4, -4,  8, -4,  8,  7,  7, -1, -2, -2, -3, -2, -3, -3, -4, -2, -3, -3, -4, -3, -4, -4,  8, -2, -3, -3, -4, -3, -4, -4,  8, -3, -4, -4,  8, -4,  8,  7,  7, -2, -3, -3, -4, -3, -4, -4,  8, -3, -4, -4,  8, -4,  8,  7,  7, -3, -4, -4,  8, -4,  8,  7,  7, -4,  8,  7,  7,  6,  6,  6,  6, -1, -2, -2, -3, -2, -3, -3, -4, -2, -3, -3, -4, -3, -4, -4,  8, -2, -3, -3, -4, -3, -4, -4,  8, -3, -4, -4,  8, -4,  8,  7,  7, -2, -3, -3, -4, -3, -4, -4,  8, -3, -4, -4,  8, -4,  8,  7,  7, -3, -4, -4,  8, -4,  8,  7,  7, -4,  8,  7,  7,  6,  6,  6,  6, -2, -3, -3, -4, -3, -4, -4,  8, -3, -4, -4,  8, -4,  8,  7,  7, -3, -4, -4,  8, -4,  8,  7,  7, -4,  8,  7,  7,  6,  6,  6,  6, -3, -4, -4,  8, -4,  8,  7,  7, -4,  8,  7,  7,  6,  6,  6,  6, -4,  8,  7,  7,  6,  6,  6,  6,  5,  5,  5,  5,  5,  5,  5,  5, };
signed char bit_poscnt6[256]= { 0, -1, -1, -2, -1, -2, -2, -3, -1, -2, -2, -3, -2, -3, -3, -4, -1, -2, -2, -3, -2, -3, -3, -4, -2, -3, -3, -4, -3, -4, -4, -5, -1, -2, -2, -3, -2, -3, -3, -4, -2, -3, -3, -4, -3, -4, -4, -5, -2, -3, -3, -4, -3, -4, -4, -5, -3, -4, -4, -5, -4, -5, -5,  8, -1, -2, -2, -3, -2, -3, -3, -4, -2, -3, -3, -4, -3, -4, -4, -5, -2, -3, -3, -4, -3, -4, -4, -5, -3, -4, -4, -5, -4, -5, -5,  8, -2, -3, -3, -4, -3, -4, -4, -5, -3, -4, -4, -5, -4, -5, -5,  8, -3, -4, -4, -5, -4, -5, -5,  8, -4, -5, -5,  8, -5,  8,  7,  7, -1, -2, -2, -3, -2, -3, -3, -4, -2, -3, -3, -4, -3, -4, -4, -5, -2, -3, -3, -4, -3, -4, -4, -5, -3, -4, -4, -5, -4, -5, -5,  8, -2, -3, -3, -4, -3, -4, -4, -5, -3, -4, -4, -5, -4, -5, -5,  8, -3, -4, -4, -5, -4, -5, -5,  8, -4, -5, -5,  8, -5,  8,  7,  7, -2, -3, -3, -4, -3, -4, -4, -5, -3, -4, -4, -5, -4, -5, -5,  8, -3, -4, -4, -5, -4, -5, -5,  8, -4, -5, -5,  8, -5,  8,  7,  7, -3, -4, -4, -5, -4, -5, -5,  8, -4, -5, -5,  8, -5,  8,  7,  7, -4, -5, -5,  8, -5,  8,  7,  7, -5,  8,  7,  7,  6,  6,  6,  6, };
signed char bit_poscnt7[256]= { 0, -1, -1, -2, -1, -2, -2, -3, -1, -2, -2, -3, -2, -3, -3, -4, -1, -2, -2, -3, -2, -3, -3, -4, -2, -3, -3, -4, -3, -4, -4, -5, -1, -2, -2, -3, -2, -3, -3, -4, -2, -3, -3, -4, -3, -4, -4, -5, -2, -3, -3, -4, -3, -4, -4, -5, -3, -4, -4, -5, -4, -5, -5, -6, -1, -2, -2, -3, -2, -3, -3, -4, -2, -3, -3, -4, -3, -4, -4, -5, -2, -3, -3, -4, -3, -4, -4, -5, -3, -4, -4, -5, -4, -5, -5, -6, -2, -3, -3, -4, -3, -4, -4, -5, -3, -4, -4, -5, -4, -5, -5, -6, -3, -4, -4, -5, -4, -5, -5, -6, -4, -5, -5, -6, -5, -6, -6,  8, -1, -2, -2, -3, -2, -3, -3, -4, -2, -3, -3, -4, -3, -4, -4, -5, -2, -3, -3, -4, -3, -4, -4, -5, -3, -4, -4, -5, -4, -5, -5, -6, -2, -3, -3, -4, -3, -4, -4, -5, -3, -4, -4, -5, -4, -5, -5, -6, -3, -4, -4, -5, -4, -5, -5, -6, -4, -5, -5, -6, -5, -6, -6,  8, -2, -3, -3, -4, -3, -4, -4, -5, -3, -4, -4, -5, -4, -5, -5, -6, -3, -4, -4, -5, -4, -5, -5, -6, -4, -5, -5, -6, -5, -6, -6,  8, -3, -4, -4, -5, -4, -5, -5, -6, -4, -5, -5, -6, -5, -6, -6,  8, -4, -5, -5, -6, -5, -6, -6,  8, -5, -6, -6,  8, -6,  8,  7,  7, };
signed char bit_poscnt8[256]= { 0, -1, -1, -2, -1, -2, -2, -3, -1, -2, -2, -3, -2, -3, -3, -4, -1, -2, -2, -3, -2, -3, -3, -4, -2, -3, -3, -4, -3, -4, -4, -5, -1, -2, -2, -3, -2, -3, -3, -4, -2, -3, -3, -4, -3, -4, -4, -5, -2, -3, -3, -4, -3, -4, -4, -5, -3, -4, -4, -5, -4, -5, -5, -6, -1, -2, -2, -3, -2, -3, -3, -4, -2, -3, -3, -4, -3, -4, -4, -5, -2, -3, -3, -4, -3, -4, -4, -5, -3, -4, -4, -5, -4, -5, -5, -6, -2, -3, -3, -4, -3, -4, -4, -5, -3, -4, -4, -5, -4, -5, -5, -6, -3, -4, -4, -5, -4, -5, -5, -6, -4, -5, -5, -6, -5, -6, -6, -7, -1, -2, -2, -3, -2, -3, -3, -4, -2, -3, -3, -4, -3, -4, -4, -5, -2, -3, -3, -4, -3, -4, -4, -5, -3, -4, -4, -5, -4, -5, -5, -6, -2, -3, -3, -4, -3, -4, -4, -5, -3, -4, -4, -5, -4, -5, -5, -6, -3, -4, -4, -5, -4, -5, -5, -6, -4, -5, -5, -6, -5, -6, -6, -7, -2, -3, -3, -4, -3, -4, -4, -5, -3, -4, -4, -5, -4, -5, -5, -6, -3, -4, -4, -5, -4, -5, -5, -6, -4, -5, -5, -6, -5, -6, -6, -7, -3, -4, -4, -5, -4, -5, -5, -6, -4, -5, -5, -6, -5, -6, -6, -7, -4, -5, -5, -6, -5, -6, -6, -7, -5, -6, -6, -7, -6, -7, -7,  8, };

signed char *bit_pos[9] = {NULL, bit_pos1, bit_pos2, bit_pos3, bit_pos4, bit_pos5, bit_pos6, bit_pos7, bit_pos8};
signed char *bit_poscnt[9] = {NULL, bit_poscnt1, bit_poscnt2, bit_poscnt3, bit_poscnt4, bit_poscnt5, bit_poscnt6, bit_poscnt7, bit_poscnt8};

/*
 * Create the table above
 */
static void __attribute__((__unused__)) UNUSED_gen_bit_count()
{
	int i, j, k, n, v;
	printf("// number of bit  set in a byte\n");
	printf("signed char bit_count[256]= { ");
	int bc[256];
	for (i=0; i<256; i++)
	{
		int c=0;
		if (i&0x01) c++;
		if (i&0x02) c++;
		if (i&0x04) c++;
		if (i&0x08) c++;
		if (i&0x10) c++;
		if (i&0x20) c++;
		if (i&0x40) c++;
		if (i&0x80) c++;
		printf("%d, ", c);
		bc[i]=c;
	}
	printf("};\n");

	printf("// This is 8 tables that holds the place of the Nth bit at ON in a byte,\n"
		   "// 0 means the first one (or the highest bit),\n"
	       "// 8 means the byte don't have enough bits at ON\n");
	for (j=0; j<8; j++)
	{
		printf("signed char bit_pos%d[256]= { ", j+1);
		printf("%d, ", 8);
		for (i=1; i<256; i++)
		{
			for (k=0, v=i, n=j+1; k<8 && 0<n; k++)
			{
				// printf("k=%d v=0x%x n=%d 0x%x\n", k, v, n, v & 0x80);
				if (v & 0x80) n--;
				v=v<<1;
			}
			if (0<n) k=9;
			printf("%d, ", k-1);
			//exit(1);
		}
		printf("};\n");
	}

	printf("// This is 8 tables that holds the place of the Nth bit at ON in a byte,\n"
		   "// 1 means the first one (or the highest bit),\n"
		   "// O or negative value means not found and contains the number of bit at ON in the bit\n"
	       "// BE CAREFULL THIS IS NOT COMPATIBLE WITH bit_pos above ! bit_poscnt[x]=1+bit_pos[x]\n");
	for (j=0; j<8; j++)
	{
		printf("signed char bit_poscnt%d[256]= { ", j+1);
		printf("%d, ", 0);
		for (i=1; i<256; i++)
		{
			for (k=0, v=i, n=j+1; k<8 && 0<n; k++)
			{
				// printf("k=%d v=0x%x n=%d 0x%x\n", k, v, n, v & 0x80);
				if (v & 0x80) n--;
				v=v<<1;
			}
			if (0<n) printf("%d, ", -bc[i]);
			else printf(" %d, ", k);
			//exit(1);
		}
		printf("};\n");
	}

}

/*
 * This APPLY a mask to drop the unused bits of the last bit_int
 * To create such a mask for further reuse, set *mask=0 before
 * use this mask only with native bit_int, not after having converted to BE
 */
static void mask_for_last(long long int size, bit_int *mask)
{
    size=size%BIT_INT_BIT_COUNT;
    int off=0;

    if (size==0) *mask=bit_int_inv;
    else
    {
        while (off<size)
        {
            bit_int v=BE_TO_BIT_INT(1<<(BIT_INT_BIT_COUNT-1-off));
            *mask=*mask | v;
            off++;
        }
    }
}

/**
 * a <=> b
 *
*/
static bit_int __attribute__((__unused__)) UNUSED_biconditional(bit_int a, bit_int b)
{
    return ((~a)|b) & (a|(~b));
}

/**
 * a => b  also, is a included in b ?
 */
static bit_int implication(bit_int a, bit_int b)
{
    return (~a)|b;
}



/**
 * compare bits in a and b
 * INCREMENT "# of bits only in a", "# of bits only in b", "# of bits both in a AND in b",
 * Dont forget to *onlya=*onlyb=*both=0 before to use this function
 */
static void bit_cmp(bit_int a, bit_int b, long long int *onlya, long long int *onlyb, long long int *both)
{
	unsigned char *pa=(unsigned char *)&a;
	unsigned char *pb=(unsigned char *)&b;

	*both+=bit_count[(*pa)&(*pb)];
	*onlya+=bit_count[(*pa)&~(*pb)];
	*onlyb+=bit_count[(*pb++)&~(*pa++)];

	*both+=bit_count[(*pa)&(*pb)];
	*onlya+=bit_count[(*pa)&~(*pb)];
	*onlyb+=bit_count[(*pb++)&~(*pa++)];

	*both+=bit_count[(*pa)&(*pb)];
	*onlya+=bit_count[(*pa)&~(*pb)];
	*onlyb+=bit_count[(*pb++)&~(*pa++)];

	*both+=bit_count[(*pa)&(*pb)];
	*onlya+=bit_count[(*pa)&~(*pb)];
	*onlyb+=bit_count[(*pb)&~(*pa)];

}

/*
 * return the number of bits set in a
 */
inline static short bit_int_count(bit_int a)
{
	// these two test speed up a lot when matched
	// and only slow down of 5 to 10% when not mached
	if (a==bit_int_zero) return 0;
	if (a==bit_int_inv) return BIT_INT_BIT_COUNT;

	signed char count;
	unsigned char *pa=(unsigned char *)&a;
	count=bit_count[*pa++];
	count+=bit_count[*pa++];
	count+=bit_count[*pa++];
	return count+bit_count[*pa];
}

/*
 * return the position of the first bit at ON in a bit_int
 * first is 0, last is 31
 * when zero, return BIT_INT_BIT_COUNT
 */
inline static int bit_int_first(bit_int a)
{
	unsigned char *pa=((unsigned char *)&a)+BIT_INT_BYTE1;
	int pos, bc;

	if ((pos=bit_pos1[*pa--])!=8) return pos;

	if ((bc=bit_pos1[*pa--])!=8) return pos+bc;
	pos+=bc;

	if ((bc=bit_pos1[*pa--])!=8) return pos+bc;
	pos+=bc;

	return pos+bit_pos1[*pa--];
}

/*
 * return the position of the Nth bit at ON in a bit_int. 0<=n<=BIT_INT_BIT_COUNT1
 * the first bit is at pos 1, (ATTN bit_int_first() would return 0 instead)
 * when not found, return a neg value, the number of bits in the bit_int
 * 0 if a==0x0
 */
inline static int bit_int_nth(bit_int a, int n)
{
	if (n==0) return 0; // I don't know what to return for now

	unsigned char *pa=(unsigned char *)&a;
	unsigned char *pi=pa+BIT_INT_BYTE1;
	int pos=0;
	int m=n;
	while (n>8 && pi>=pa)
	{
		n-=bit_count[*pi--];
		pos+=8;
	}
	while (n>0 && pi>=pa)
	{
		int p=bit_poscnt[n][*pi--];
		if (p>0) return pos+p; // found
		n+=p;
		pos+=8;
	}
	return n-m; // not found, return neg value=the number of bit in the bit_int
}

/*
 * allocate and initialize a bit_array
 */
int bit_array_init(struct bit_array *ba, long long int size, int zero_or_one)
{
    ba->size=size;
    ba->isize=(size+BIT_INT_BIT_COUNT1)/BIT_INT_BIT_COUNT;
    ba->allocated=0;
    ba->array=malloc(ba->isize*BIT_INT_BYTE);
    if (ba->array==NULL) return 1;
    ba->allocated=1;
    ba->end=ba->array+ba->isize;
    ba->last=ba->end-1;

    ba->index=0;
    ba->mask_last=0;
    mask_for_last(size, &ba->mask_last);
    bit_array_reset(ba, zero_or_one);
    return 0;
}

/*
 * initialize a bit_array using an already allocated and initialized 'buffer'
 */
int bit_array_init2(struct bit_array *ba, long long int size, void *buffer)
{
    ba->size=size;
    ba->isize=(size+BIT_INT_BIT_COUNT1)/BIT_INT_BIT_COUNT;
    ba->allocated=0;
    ba->array=(bit_int *)buffer;
    ba->end=ba->array+ba->isize;
    ba->last=ba->end-1;
    ba->index=0;
    ba->mask_last=0;
    mask_for_last(size, &ba->mask_last);
    return 0;
}
/*
 * release allocated space (when allocated by the library)
 */
void bit_array_release(struct bit_array *ba)
{
    if (ba->allocated) free(ba->array);
}

void bit_array_reset(struct bit_array *ba, int zero_or_one)
{
    bit_int value;
    if (zero_or_one) value=bit_int_inv;
    else value=bit_int_zero;
    bit_int *pi=ba->array;
    while (pi<ba->end) *pi++=value;
}

void bit_array_random(struct bit_array *ba)
{
    bit_int *pi=ba->array;
    while (pi<ba->end) *pi++=random();
}

/*
 * set a bit at ON and return 0 if it was OFF or !=0 if it was ON
 */
int bit_array_set(struct bit_array *ba, long long int bit_addr)
{
	//assert(bit_addr<ba->size);
    bit_int *p=ba->array+(bit_addr>>BIT_INT_SHIFT);
    bit_int v=BIT_INT_TO_BE(BIT_INT_HIGHEST_BIT>>(bit_addr&BIT_INT_OFF_MASK));
    int res=(*p & v)!=bit_int_zero;
    *p|=v;
    return res;
}

/*
 * set a bit at OFF and return 0 if it was OFF or !=0 if it was ON
 */
int bit_array_unset(struct bit_array *ba, long long int bit_addr)
{
	//assert(bit_addr<ba->size);
    bit_int *p=ba->array+(bit_addr>>BIT_INT_SHIFT);
    bit_int v=BIT_INT_TO_BE(~(BIT_INT_HIGHEST_BIT>>(bit_addr&BIT_INT_OFF_MASK)));
    int res=(*p & ~v)!=bit_int_zero;
    *p&=v;
    return res;
}
/*
 * return 0 if bit at bit_addr is OFF
 */
int bit_array_get(struct bit_array *ba, long long int bit_addr)
{
    // assert(bit_addr<ba->size);
    return (ba->array[bit_addr>>BIT_INT_SHIFT] & BIT_INT_TO_BE(BIT_INT_HIGHEST_BIT>>(bit_addr&BIT_INT_OFF_MASK)))!=0;
}

/*
 * reset a zone, from and to are included
 */
void bit_array_reset_zone(struct bit_array *ba, long long int from, long long int to, int set)
{
	// assert(from<=to && to<ba->size);
    int foff=from&BIT_INT_OFF_MASK;
    int toff=to&BIT_INT_OFF_MASK;

    bit_int *pi=ba->array+(from>>BIT_INT_SHIFT);
    bit_int *pe=ba->array+(to>>BIT_INT_SHIFT);

    bit_int mask=bit_int_inv<<foff>>foff;
    if (pi==pe)
    {
        mask=mask>>(BIT_INT_BIT_COUNT1-toff)<<(BIT_INT_BIT_COUNT1-toff);
        if (set) *pi|=BIT_INT_TO_BE(mask);
        else *pi&=BIT_INT_TO_BE(~mask);
        return;
    }
    if (set) *pi|=BIT_INT_TO_BE(mask);
    else *pi&=BIT_INT_TO_BE(~mask);
    pi++;

    if (set) mask=bit_int_inv;
    else mask=bit_int_zero;
    while (pi<pe) *pi++=mask;

    mask=bit_int_inv>>(BIT_INT_BIT_COUNT1-toff)<<(BIT_INT_BIT_COUNT1-toff);
    if (set) *pe|=BIT_INT_TO_BE(mask);
    else *pe&=BIT_INT_TO_BE(~mask);
}


void bit_array_print(struct bit_array *ba)
{
    long long int i;
    long long int k=ba->size;
    int j;

    printf("%lld %lld : ", ba->size, ba->isize);

    for (i=0; i<ba->isize; i++)
    {
        bit_int v=BE_TO_BIT_INT(ba->array[i]);
        for (j=0; j<BIT_INT_BIT_COUNT && k>0; j++, k--)
        {
            if (v & BIT_INT_HIGHEST_BIT) printf("1");
            else printf("0");
            v=v<<1;
        }
    }
    printf("\n");
}

long long int bit_array_search_first_set(struct bit_array *ba, long long int from)
{
	long long int addr;

    int off=from&BIT_INT_OFF_MASK;

    bit_int *pv=ba->array+(from>>BIT_INT_SHIFT);
    bit_int v=BE_TO_BIT_INT(*pv++)<<off>>off;
    if (v!=bit_int_zero)
    {
    	addr=from-off+bit_int_first(v);
    	if (addr<ba->size) return addr;
    	else return -1;
    }

    while (pv<ba->end && *pv==bit_int_zero) pv++;

    if (pv<ba->end)
    {
    	addr=(pv-ba->array)*BIT_INT_BIT_COUNT+bit_int_first(BE_TO_BIT_INT(*pv));
        if (addr<ba->size) return addr;
    }
    return -1;
}

long long int bit_array_search_first_unset(struct bit_array *ba, long long int from)
{
	long long int addr;
    if (from>=ba->size) return -1;  // 'from' is too far

    int off=from&BIT_INT_OFF_MASK;

    bit_int *pv=ba->array+(from>>BIT_INT_SHIFT);
    bit_int v=~BE_TO_BIT_INT(*pv++)<<off>>off;
    if (v!=bit_int_zero)
    {
    	addr=from-off+bit_int_first(v);
    	if (addr<ba->size) return addr;
    	else return -1;
    }

    while (pv<ba->end && *pv==bit_int_inv) pv++;

    if (pv<ba->end)
    {
    	addr=(pv-ba->array)*BIT_INT_BIT_COUNT+bit_int_first(~BE_TO_BIT_INT(*pv));
        if (addr<ba->size) return addr;
    }
    return -1;
}

/* search the N'th bit set after from */
long long int bit_array_search_nth_set(struct bit_array *ba, long long int from, long long int n)
{
	long long int addr;
	if (n==0) return 0; // useful for the quick functions
    if (from>=ba->size) return -1;  // 'from' is too far

    int off=from&BIT_INT_OFF_MASK;

    bit_int *pv=ba->array+(from>>BIT_INT_SHIFT);

    // handle part of the first bit_int
    int bc;
    if (n<=BIT_INT_BIT_COUNT)
    {
    	bc=bit_int_nth(BE_TO_BIT_INT(*pv)<<off>>off, n);
    	if (bc>0) goto FOUND;
        n+=bc; // bc<=0
    }
    else n-=bit_int_count(BE_TO_BIT_INT(*pv)<<off);

    pv++;
    // handle bit_int at once when possible
    while (n>BIT_INT_BIT_COUNT && pv<ba->end)
	{
    	if (*pv==bit_int_inv) n-=BIT_INT_BIT_COUNT;
    	else if (*pv!=bit_int_zero) n-=bit_int_count(*pv);
    	pv++;
	}
    // n<=BIT_INT_BIT_COUNT
    while (n>0 && pv<ba->end)
	{
        bc=bit_int_nth(BE_TO_BIT_INT(*pv), n);
        if (bc>0) goto FOUND;
        n+=bc; // bc<=0
    	pv++;
	}

    return -1;
FOUND:
	addr=(pv-ba->array)*BIT_INT_BIT_COUNT+bc-1;
	if (addr<ba->size) return addr;
	return -1;
}


/* search the N'th bit unset after from */
long long int bit_array_search_nth_unset(struct bit_array *ba, long long int from, long long int n)
{
	long long int addr;

	if (n==0) return from; // useful for the quick functions
    if (from>=ba->size) return -1;  // 'from' is too far

    int off=from&BIT_INT_OFF_MASK;

    bit_int *pv=ba->array+(from>>BIT_INT_SHIFT);

    // handle part of the first bit_int
    int bc;
    if (n<=BIT_INT_BIT_COUNT)
    {
    	bc=bit_int_nth(BE_TO_BIT_INT(~*pv)<<off>>off, n);
    	if (bc>0) goto FOUND;
        n+=bc; // bc<=0
    }
    else n-=bit_int_count(BE_TO_BIT_INT(~*pv)<<off);

    pv++;
    // handle bit_int at once when possible
    while (n>BIT_INT_BIT_COUNT && pv<ba->end)
	{
    	if (*pv==bit_int_zero) n-=BIT_INT_BIT_COUNT;
    	else if (*pv!=bit_int_inv) n-=BIT_INT_BIT_COUNT-bit_int_count(*pv);
    	pv++;
	}
    // n<=BIT_INT_BIT_COUNT
    while (n>0 && pv<ba->end)
	{
        bc=bit_int_nth(BE_TO_BIT_INT(~*pv), n);
        if (bc>0) goto FOUND;
        n+=bc; // bc<=0
    	pv++;
	}

    return -1;
FOUND:
	addr=(pv-ba->array)*BIT_INT_BIT_COUNT+bc-1;
	if (addr<ba->size) return addr;
	return -1;
}


/*
 * Count the number of bits set between 2 addresses from and to
 */
long long int bit_array_count_zone(struct bit_array *ba, long long int from, long long int to)
{
    long long int count;

    if (from>to || to>=ba->size) return -1;  // 'from' is too far

    int foff=from&BIT_INT_OFF_MASK;
    int toff=to&BIT_INT_OFF_MASK;

    bit_int *pi=ba->array+(from>>BIT_INT_SHIFT);
    bit_int *pe=ba->array+(to>>BIT_INT_SHIFT);

    // take relevant part of the first bit_int
    bit_int v=BE_TO_BIT_INT(*pi)<<foff;
    if (pi==pe)
    {
        v=v>>(foff+BIT_INT_BIT_COUNT1-toff);
    	return bit_int_count(v);
    }
    count=bit_int_count(v);
    pi++;
    while (pi<pe) count+=bit_int_count(*pi++);
    return count+bit_int_count(BE_TO_BIT_INT(*pe)>>(BIT_INT_BIT_COUNT1-toff));
}

void bit_array_inverse_into(struct bit_array *src, struct bit_array *dst)
{
    bit_int *p, *q;
    for (p=src->array, q=dst->array; p<src->end; p++, q++) *q=~*p;
}

void bit_array_bwand(struct bit_array *src, struct bit_array *dst)
{
    bit_int *p, *q;
    for (p=src->array, q=dst->array; p<src->end; p++, q++) (*q)&=*p;
}

void bit_array_bwor(struct bit_array *src, struct bit_array *dst)
{
    bit_int *p, *q;
    for (p=src->array, q=dst->array; p<src->end; p++, q++) (*q)|=*p;
}

void bit_array_plus_diff(struct bit_array *a, struct bit_array *b, struct bit_array *c)
{ // a=a+(b-c)
    bit_int *p, *q, *r;
    for (p=a->array, q=b->array, r=c->array; p<a->end; p++, q++, r++) (*p)|=*q & ~*r;
}


void bit_array_copy(struct bit_array *src, struct bit_array *dst)
{
    bit_int *p, *q;
    for (p=src->array, q=dst->array; p<src->end; p++, q++) *q=*p;
}

/**
 * compare 2 bit array
 *
 * if (bit_array_cmp(a, b, &cmp) && cmp==0) then equal
 * if (bit_array_cmp(a, b, &cmp) && cmp<0) then a<b
 * if (bit_array_cmp(a, b, &cmp) && cmp>0) then a>b
 * if (!bit_array_cmp(a, b, &cmp)) then nothing to say on a and b
 *
 * @param ba1
 * @param ba2
 * @param cmp if one is included into the other, return -1, 1 or 0 if
 * respectively ba1<ba2, ba1>ba2, ba1==ba2 otherwise 0
 * @return 1 if one is included into the other, 0 otherwise
 */
int bit_array_cmp(struct bit_array *ba1, struct bit_array *ba2, int *cmp)
{
    bit_int *p, *q;
    bit_int cmp1=bit_int_inv;
    bit_int cmp2=bit_int_inv;

    for (p=ba1->array, q=ba2->array; p<ba1->last; p++, q++)
    {
        cmp1&=implication(*p, *q);
        cmp2&=implication(*q, *p);
    }
    // use mask for the last one
    cmp1&=implication((*p)&ba1->mask_last, (*q)&ba2->mask_last);
    cmp2&=implication((*q)&ba2->mask_last, (*p)&ba1->mask_last);

    cmp1=(cmp1==bit_int_inv);
    cmp2=(cmp2==bit_int_inv);

    *cmp=0;
    if (cmp1) (*cmp)--;
    if (cmp2) (*cmp)++;
    if (cmp1 || cmp2) return 1;
    return 0;
}

/**
 * bit_array_cmp_count()
 * compare 2 bit array, 4 time slower than bit_array_cmp()
 * but return accounting
 *
 * @param ba1
 * @param ba2
 * @param only1 number of bits only in ba1
 * @param only2 number of bits only in ba2
 * @param both number of bits in both
 * @return 1 if ba1==ba2
 */
int bit_array_cmp_count(struct bit_array *ba1, struct bit_array *ba2, long long int *only1, long long int *only2, long long int *both)
{
	*only1=*only2=*both=0LL;
	int equal=1;

    bit_int *p, *q;
    for (p=ba1->array, q=ba2->array; p<ba1->last; p++, q++)
    {
    	bit_cmp(*p, *q, only1, only2, both);
    	if (*p!=*q) equal=0;
    }
    // use mask for the last one
    bit_cmp((*p)&ba1->mask_last, (*q)&ba2->mask_last, only1, only2, both);
    if (((*p)&ba1->mask_last)!=((*q)&ba2->mask_last)) equal=0;
    return equal;
}

/*
 * search and set the first free bit
 * use internal value 'index' to start the search and not always start from 0
 */
long long int bit_array_alloc(struct bit_array *ba)
{
    long long int addr=bit_array_search_first_unset(ba, ba->index);
    if (addr==-1) addr=bit_array_search_first_unset(ba, 0);

    if (addr==-1) ba->index=0;
    else
    {
        if (addr<ba->size) ba->index=addr+1;
        else ba->index=0;
        bit_array_set(ba, addr);
    }
    return addr;
}

/*
 * Count the number of bit set and unset
 */
void bit_array_count(struct bit_array *ba, long long int *set, long long int *unset)
{
    bit_int *pi=ba->array;

    *set=0;
    while (pi<ba->last) *set+=bit_int_count(*pi++);
    *set+=bit_int_count(*pi&ba->mask_last);
    *unset=ba->size-*set;
}


int bit_array_save(struct bit_array *ba, char *filename, int sync)
{
    int fd=open(filename, O_RDWR|O_CREAT|O_TRUNC, 0600);
    if (fd==-1) return -errno;
    int size=ba->isize*BIT_INT_BYTE;
    int len=write(fd, ba->array, size);
    if (len==size && sync) fsync(fd);
    close(fd);

    if (len==-1) return -errno;
    if (len!=size) return 1;
    return 0;
}

int bit_array_load(struct bit_array *ba, char *filename)
{
    int fd=open(filename, O_RDONLY);
    if (fd==-1) return -errno;
    int size=ba->isize*BIT_INT_BYTE;
    int len=read(fd, ba->array, size);
    close(fd);

    if (len==-1) return -errno;
    if (len!=size) return 1;
    return 0;
}

/*
 * quick_bit_array_init
 *
 * if size<0 then size means the number of bits per index entry, for example 400
 */
int quick_bit_array_init(struct quick_bit_array *q, struct bit_array *ba, long long int size, long long int from, long long int to, int what)
{
	long long int i;
	// fprintf(stderr, "quick_bit_array_init size=%lld from=%lld to=%lld\n", size, from, to);

	q->ba=ba;
	if (size<0)
	{
		if (what==qba_count_zone) size=(to-from+1)/-size;
		else if (what==qba_nth_unset) size=(to-from+1-bit_array_count_zone(ba, from, to))/-size;
	}
	q->size=size;
	if (from>to) return 1;
	if (to>q->ba->size) return 1;
	q->from=from;
	q->to=to;
	q->quick=malloc(q->size*sizeof(long long int));
	if (q->quick==NULL) return 1;

	if (what==qba_count_zone)
	{
		q->coef=(q->to-q->from+1+q->size-1)/q->size;
		// from, to are bit addresses (from=used_block, to=block_count)
        q->quick[0]=bit_array_count_zone(q->ba, q->from, q->from); // yes from and from
    	long long int paddr=q->from; // previous bit address
		for (i=1; i<q->size; i++)
		{
			// quick_addr_count()
			// [ 0, quick_size [ -> addr in [used, block_count[
			// quick_addr_count(block_count, addr)=quick_addr_count(block_count, paddr)+quick_addr_count(paddr+1, addr)
			long long int addr=from+i*q->coef;
			// q->quick[i]=bit_array_count_zone(&q->ba, q->from, addr);
			q->quick[i]=q->quick[i-1]+bit_array_count_zone(q->ba, paddr+1, addr);
			paddr=addr;
		}
	}
	else if (what==qba_nth_unset)
	{
		// number of unset bits betwenn from and to
		q->max=q->to-q->from+1-bit_array_count_zone(q->ba, q->from, q->to);
/*
		long long int tmp=bit_array_search_nth_unset(q->ba, q->from, q->max);
		fprintf(stderr, "max=%lld %lld %lld\n", q->max, tmp, q->to);
		assert(tmp<=q->to);
		tmp=bit_array_search_nth_unset(q->ba, tmp, 2);
		fprintf(stderr, "tmp=%lld\n", tmp);
		assert(tmp==-1 || tmp>q->to);
*/
		if (q->max!=0)
		{
			q->coef=(q->max+q->size-1)/q->size;
			// from and to are a count = number of bit that are unset between from and to

//			fprintf(stderr, "start=%lld from=%lld\n", q->from-q->from%100, q->from);
//			for (i=0; i<100; i++) if (i%10==0) fprintf(stderr, "%lld", i/10); else fprintf(stderr, " ");
//			fprintf(stderr, "\n");
//			for (i=0; i<100; i++) fprintf(stderr, "%lld", i%10);
//			for (i=q->from-q->from%100; i<q->from-q->from%100+300; i++)
//			{
//				if (i%100==0) fprintf(stderr, "\n");
//				if (bit_array_get(q->ba, i)) fprintf(stderr, "1");
//				else fprintf(stderr, "0");
//			}
//			fprintf(stderr, "\n");
			q->quick[0]=bit_array_search_nth_unset(q->ba, q->from, 1);
//			fprintf(stderr, "quick[0]=%lld\n", q->quick[0]);
			long long int count;
			for (i=1, count=q->coef+1; count<=q->max; i++, count+=q->coef)
			{
				// quick_count_addr()
				// [ 0, quick_size [ -> count in [0, q->diff(=used) ]
				// quick_count_addr(from_addr, count)=quick_count_addr(quick_count_addr(from_addr, pcount), count-pcount+1))
				// q->quick[i]=bit_array_search_nth_unset(&ba, q->from, count);
	//			fprintf(stderr, "qba_nth_unset i=%lld/%lld count=%lld from=%lld to=%lld\n", i, q->size, count, from, to);
	//			fprintf(stderr, "bit_array_search_nth_unset from=%lld to=%lld\n", q->quick[i-1], count-pcount+1);
				q->quick[i]=bit_array_search_nth_unset(q->ba, q->quick[i-1]+1, q->coef);
//				if (q->quick[i]!=bit_array_search_nth_unset(q->ba, q->from, count))
//				{
//if (from==12822 && to==70889 && count>30000-q->coef)
//				fprintf(stderr, "bit_array_search_nth_unset i=%lld/%lld count=%lld quick=%lld nth_unset=%lld !=%lld inc=%lld prev=%lld\n", i, q->size, count, q->quick[i], bit_array_search_nth_unset(q->ba, q->from, count), q->quick[i]-bit_array_search_nth_unset(q->ba, q->from, count), q->coef, q->quick[i-1]);
//
//				}
			}
		}
    }
	return 0;
}

void quick_bit_array_release(struct quick_bit_array *q)
{
	free(q->quick);
}

long long int quick_bit_array_count_zone(struct quick_bit_array *q, long long int bit_addr)
{
	long long int iaddr=(bit_addr-q->from)/q->coef;
	long long int naddr=q->from+iaddr*q->coef+1;
	long long int count=q->quick[iaddr];
	if (naddr<=bit_addr) count+=bit_array_count_zone(q->ba, naddr, bit_addr);
	// assert(count==bit_array_count_zone(q->ba, q->from, bit_addr));
	return count;
}

long long int quick_bit_array_search_nth_unset(struct quick_bit_array *q, long long int count)
{
	// return bit_array_search_nth_unset(q->ba, q->from, count);
	if (count>q->max) return -1; // The zone don't have enough unset bits
	if (count==0) return q->from;
	count--;
//	fprintf(stderr, "idx=%lld gap=%lld quick=%lld\n", count/q->coef, count%q->coef+1, q->quick[count/q->coef]);
	long long int bit_addr=bit_array_search_nth_unset(q->ba, q->quick[count/q->coef], count%q->coef+1);
	// assert(bit_addr==bit_array_search_nth_unset(q->ba, q->from, count));
	return bit_addr;
}

static long long int now()
{
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return tv.tv_sec*1000LL+tv.tv_usec/1000LL;
}

static void test_bit_cmp(bit_int a, bit_int b)
{
	long long int first, second, both;
	first=second=both=0LL;
	bit_cmp(a, b, &first, &second, &both);
	printf("cmp(0x%llx, 0x%llx) -> ( %lld, %lld, %lld )\n", (long long int)a, (long long int)b, first, second, both);
}

/*
 * the main() function when testing is required.
 */
int ma_main(int argc, char *argv[])
{
    struct bit_array ba, bb, bc, bd, be, bf;
    int test_size=60;
    int test_size_med=1000;

    int i, j;
    long long int start, end, last;
    long long int a, b;

    // if (1) gen_bit_count();

    if (1)
    {
        bit_array_init(&ba, test_size, 0);

        // test bit_count[]
    	assert(bit_count[0x00]==0);
    	assert(bit_count[0x10]==1);
    	assert(bit_count[0x14]==2);
    	assert(bit_count[0x1A]==3);
    	assert(bit_count[0x3A]==4);
    	assert(bit_count[0x73]==5);
    	assert(bit_count[0xE7]==6);
    	assert(bit_count[0xFE]==7);
    	assert(bit_count[0xFF]==8);
    	// test bit_pos*[] & bit_poscnt*[]
    	assert(bit_pos1[0x00]==8 && bit_poscnt1[0x00]==0);
    	assert(bit_pos1[0x01]==7 && bit_poscnt1[0x01]==8);
    	assert(bit_pos1[0x02]==6 && bit_poscnt1[0x02]==7);
    	assert(bit_pos1[0x03]==6 && bit_poscnt1[0x03]==7);
    	assert(bit_pos1[0xFF]==0 && bit_poscnt1[0xFF]==1);

    	assert(bit_pos2[0x00]==8 && bit_poscnt2[0x00]==0);
    	assert(bit_pos2[0x01]==8 && bit_poscnt2[0x01]==-1);
    	assert(bit_pos2[0x02]==8 && bit_poscnt2[0x02]==-1);
    	assert(bit_pos2[0x03]==7 && bit_poscnt2[0x03]==8);
    	assert(bit_pos2[0xFF]==1 && bit_poscnt2[0xFF]==2);

    	assert(bit_pos3[0x00]==8 && bit_poscnt3[0x00]==0);
    	assert(bit_pos3[0x01]==8 && bit_poscnt3[0x01]==-1);
    	assert(bit_pos3[0x03]==8 && bit_poscnt3[0x03]==-2);
    	assert(bit_pos3[0xAA]==4 && bit_poscnt3[0xAA]==5);
    	assert(bit_pos3[0xFF]==2 && bit_poscnt3[0xFF]==3);

    	assert(bit_pos4[0x0F]==7 && bit_poscnt4[0x0F]==8);
    	assert(bit_pos5[0x3E]==6 && bit_poscnt5[0x3E]==7);
    	assert(bit_pos6[0x7F]==6 && bit_poscnt6[0x7F]==7);
    	assert(bit_pos7[0x6F]==8 && bit_poscnt7[0x6F]==-6);

    	assert(bit_pos8[0xFE]==8 && bit_poscnt8[0xFE]==-7);
    	assert(bit_pos8[0xFF]==7 && bit_poscnt8[0xFF]==8);

    	// mask_for_last
        for (i=0; i<33; i++)
        {
            bit_int mask=0;
            mask_for_last(i, &mask);
            if (i>0) bit_array_set(&ba, i-1);
            // printf("mask %3d 0x%08x 0x%08x 0x%08x\n", i, mask, ba.array[0], (mask & ba.array[0]));
            assert((mask&ba.array[0])==ba.array[0]);
        }

    	assert(bit_int_count(0x0)==0);
    	assert(bit_int_count(0x1)==1);
    	assert(bit_int_count(BIT_INT_TO_BE(0x1))==1);
    	assert(bit_int_count(BIT_INT_HIGHEST_BIT)==1);
    	assert(bit_int_count(BIT_INT_TO_BE(BIT_INT_HIGHEST_BIT))==1);
    	assert(bit_int_count(0x10101010)==4);
    	assert(bit_int_count(bit_int_zero)==0);
    	assert(bit_int_count(bit_int_inv)==BIT_INT_BIT_COUNT);
    	assert(bit_int_count(0x11111111)==8);
    	assert(bit_int_count(0x01010101)==4);
    	assert(bit_int_count(0x00001FF1)==10);
    	assert(bit_int_count(0x0001FF10)==10);
    	assert(bit_int_count(0x001FF100)==10);
    	assert(bit_int_count(0x01FF1000)==10);
    	assert(bit_int_count(0x1FF10000)==10);

    	assert(bit_int_first(0x0)==BIT_INT_BIT_COUNT);
    	assert(bit_int_first(0x1)==31);
    	assert(bit_int_first(BIT_INT_HIGHEST_BIT)==0);

    	assert(bit_int_first(0x10101010)==3);
    	assert(bit_int_first(bit_int_zero)==BIT_INT_BIT_COUNT);
    	assert(bit_int_first(bit_int_inv)==0);
    	assert(bit_int_first(0x11111111)==3);
    	assert(bit_int_first(0x01010101)==7);
    	assert(bit_int_first(0x00001FF1)==19);
    	assert(bit_int_first(0x0001FF10)==15);
    	assert(bit_int_first(0x001FF100)==11);
    	assert(bit_int_first(0x01FF1000)==7);
    	assert(bit_int_first(0x1FF10000)==3);

//    	fprintf(stderr, "%d %d\n", bit_int_nth(bit_int_zero, 1), bit_int_first(bit_int_zero));
    	assert(bit_int_nth(0x10101010, 1)==bit_int_first(0x10101010)+1);
//    	assert(bit_int_nth(bit_int_zero, 1)==bit_int_first(bit_int_zero)+1);
    	assert(bit_int_nth(bit_int_inv, 1)==bit_int_first(bit_int_inv)+1);
    	assert(bit_int_nth(0x11111111, 1)==bit_int_first(0x11111111)+1);
    	assert(bit_int_nth(0x01010101, 1)==bit_int_first(0x01010101)+1);
    	assert(bit_int_nth(0x00001FF1, 1)==bit_int_first(0x00001FF1)+1);
    	assert(bit_int_nth(0x0001FF10, 1)==bit_int_first(0x0001FF10)+1);
    	assert(bit_int_nth(0x001FF100, 1)==bit_int_first(0x001FF100)+1);
    	assert(bit_int_nth(0x01FF1000, 1)==bit_int_first(0x01FF1000)+1);
    	assert(bit_int_nth(0x1FF10000, 1)==bit_int_first(0x1FF10000)+1);

//    	fprintf(stderr, "%d\n", bit_int_nth(0x80101001, 0));
    	assert(bit_int_nth(0x10101010, 3)==20);
//    	assert(bit_int_nth(bit_int_zero, 1)==BIT_INT_BIT_COUNT);
    	assert(bit_int_nth(bit_int_inv, 1)==1);
    	assert(bit_int_nth(bit_int_inv, 2)==2);
    	assert(bit_int_nth(bit_int_inv, 11)==11);
    	assert(bit_int_nth(bit_int_inv, 23)==23);
    	assert(bit_int_nth(bit_int_inv, 24)==24);
    	assert(bit_int_nth(bit_int_inv, 25)==25);
    	assert(bit_int_nth(0x11111111, 4)==16);
    	assert(bit_int_nth(0x01010101, 4)==32);
    	assert(bit_int_nth(0x00001FF1, 2)==21);
    	assert(bit_int_nth(0x0001FF10, 2)==17);
    	assert(bit_int_nth(0x001FF100, 2)==13);
    	assert(bit_int_nth(0x01FF1000, 2)==9);
    	assert(bit_int_nth(0x1FF10000, 2)==5);

    	assert(bit_int_nth(0x11111111, 9)==-8);
    	assert(bit_int_nth(0x01010101, 5)==-4);
    	assert(bit_int_nth(0x00001FF1, 11)==-10);
    	assert(bit_int_nth(0x0001FF10, 11)==-10);
    	assert(bit_int_nth(0x001FF100, 11)==-10);
    	assert(bit_int_nth(0x01FF1000, 11)==-10);
    	assert(bit_int_nth(0x1FF10000, 11)==-10);

    	bit_int zero=0;
    	bit_int full=~(bit_int)0;
    	bit_int two=0x10;
    	bit_int forth=0x1010;

    	test_bit_cmp(zero, zero);
    	test_bit_cmp(zero, two);
    	test_bit_cmp(zero, forth);
    	test_bit_cmp(zero, full);

    	test_bit_cmp(two, zero);
    	test_bit_cmp(two, two);
    	test_bit_cmp(two, forth);
    	test_bit_cmp(two, full);

    	test_bit_cmp(forth, zero);
    	test_bit_cmp(forth, two);
    	test_bit_cmp(forth, forth);
    	test_bit_cmp(forth, full);

    	test_bit_cmp(full, zero);
    	test_bit_cmp(full, two);
    	test_bit_cmp(full, forth);
    	test_bit_cmp(full, full);

        bit_array_init(&ba, test_size, 0);

        assert(bit_array_set(&ba, 1)==0);
        assert(bit_array_set(&ba, 3)==0);
        assert(bit_array_set(&ba, 6)==0);
        assert(bit_array_set(&ba, 7)==0);
        assert(bit_array_set(&ba, 7)!=0);

        assert(bit_array_get(&ba, 1)!=0);

    }

    if (1)
    {
        int value[]={ 1, 58, 4, 55, 13, 59, 0, 57, -1};
        long long int set, unset;

        bit_array_init(&ba, test_size, 0);
        for (i=0; i<33; i++)
        {
            bit_int mask=0;
            mask_for_last(i, &mask);
            if (i>0) bit_array_set(&ba, i-1);
            // printf("mask %3d 0x%08x 0x%08x 0x%08x\n", i, mask, ba.array[0], (mask & ba.array[0]));
            assert((mask&ba.array[0])==ba.array[0]);
        }

        bit_array_init(&ba, test_size, 0);
        bit_array_init(&bb, test_size, 0);

        int cmp;

        assert(bit_array_cmp(&ba, &bb, &cmp) && cmp==0);
        assert(bit_array_cmp(&bb, &ba, &cmp) && cmp==0);

        for (i=0; value[i]!=-1; i++)
        {
            //printf("%d %d\n", i, value[i]);
            assert(bit_array_get(&ba, value[i])==0);
            assert(bit_array_set(&ba, value[i])==0);
            assert(bit_array_get(&ba, value[i])!=0);
            assert(bit_array_unset(&ba, value[i])!=0);
            assert(bit_array_get(&ba, value[i])==0);
            assert(bit_array_set(&ba, value[i])==0);
            assert(bit_array_cmp(&ba, &bb, &cmp) && cmp>0);
            assert(bit_array_cmp(&bb, &ba, &cmp) && cmp<0);

            assert(bit_array_set(&bb, value[i])==0);
            assert(bit_array_cmp(&ba, &bb, &cmp) && cmp==0);
            assert(bit_array_cmp(&bb, &ba, &cmp) && cmp==0);

            bit_array_count(&ba, &set, &unset);
            assert(set==i+1);
            assert(set+unset==test_size);

        }
        //printf("test cmp finished: ok\n");
        bit_array_release(&ba);
        bit_array_release(&bb);
    }

    if (1)
    {
        int value[]={ 1, 4, 13, 32, 55, 57, 58, 59, -1};

        bit_array_init(&ba, test_size, 0);
        bit_array_init(&bb, test_size, 1);

        for (i=0; value[i]!=-1; i++)
        {
        	bit_array_set(&ba, value[i]);
        	bit_array_unset(&bb, value[i]);
        }
        int n=i;

        long long int onlya, onlyb, both;
        assert(0==bit_array_cmp_count(&ba, &bb, &onlya, &onlyb, &both));
        assert(both==0 && onlya+onlyb==ba.size);
        assert(1==bit_array_cmp_count(&ba, &ba, &onlya, &onlyb, &both));
        assert(both==n && onlya+onlyb==0);
        assert(1==bit_array_cmp_count(&bb, &bb, &onlya, &onlyb, &both));
        assert(both==ba.size-n && onlya+onlyb==0);

        int *verify0=malloc(ba.size*sizeof(int));
        int *verify1=malloc(ba.size*sizeof(int));
        int count=0;
        for (i=0; i<ba.size; i++)
        {
        	verify0[i]=count;
        	if (bit_array_get(&ba, i)) count++;
        	verify1[i]=count;
        }

        for (i=0; i<ba.size; i++)
        {
        	a=bit_array_search_first_set(&ba, i);
        	if (a>=0) assert(0!=bit_array_get(&ba, a) && 1==verify1[a]-verify0[i]);
        	else assert(verify1[i]==n-1);
        	for (j=1; j<=verify1[n-1]-verify1[i]; j++)
        	{
            	a=bit_array_search_nth_set(&ba, i, j);
            	if (a>=0) assert(0!=bit_array_get(&ba, a) && j==verify1[a]-verify0[i]);
            	else assert(verify1[n-1]-verify0[i]>j);
            	b=bit_array_search_nth_unset(&bb, i, j);
            	assert(a==b);
        	}
            for (j=i; j<ba.size; j++)
            {
            	// fprintf(stderr, "%d %d %lld=%d-%d\n", i, j, bit_array_count_zone(&ba, i, j), verify1[j], verify0[i]);
            	assert(bit_array_count_zone(&ba, i, j)==verify1[j]-verify0[i]);
            }

        }
        //printf("test cmp finished: ok\n");
    }

    if (1)
    {
    	// random test zone
        bit_array_init(&ba, test_size_med, 0);

        for (i=0; i<100*test_size_med; i++)
        {
			a=random()%test_size_med;
			b=random()%test_size_med;
			if (a>b) { int t=a; a=b; b=t; }

			bit_array_reset(&ba, 0);
            bit_array_reset_zone(&ba, a, b, 1);
            assert(a==bit_array_search_first_set(&ba, 0));
            if (a<ba.size-1)
            {
            	if (b+1<ba.size)
            	{
            		assert(b+1==bit_array_search_first_unset(&ba, a));
            		assert(-1==bit_array_search_first_set(&ba, b+1));
            	}
            	else assert(-1==bit_array_search_first_unset(&ba, a));
            }
            assert(b-a+1==bit_array_count_zone(&ba, a, b));
            assert(b-a+1==bit_array_count_zone(&ba, 0, ba.size-1));

			bit_array_reset(&ba, 1);
            bit_array_reset_zone(&ba, a, b, 0);
            assert(a==bit_array_search_first_unset(&ba, 0));
            if (a<ba.size-1)
            {
            	if (b+1<ba.size)
            	{
            		assert(b+1==bit_array_search_first_set(&ba, a));
            		assert(-1==bit_array_search_first_unset(&ba, b+1));
            	}
            	else assert(-1==bit_array_search_first_set(&ba, a));
            }
            assert(0==bit_array_count_zone(&ba, a, b));
            assert(ba.size-b+a-1==bit_array_count_zone(&ba, 0, ba.size-1));
        }
        bit_array_release(&ba);
        bit_array_release(&bb);

    }

    if (1)
    {	// test copy, bwand, bwor, plus_diff
    	long long int o1, o2, both;
    	int cmp;

        bit_array_init(&ba, test_size_med, 0);
        bit_array_init(&bb, test_size_med, 0);
        bit_array_init(&bc, test_size_med, 0);
        bit_array_init(&bd, test_size_med, 0);
        bit_array_init(&be, test_size_med, 0);
        bit_array_init(&bf, test_size_med, 0);

        for (i=0; i<10000; i++)
        {
			bit_array_random(&ba);
			bit_array_inverse_into(&ba, &bb);
			bit_array_inverse_into(&bb, &bc);
			bit_array_copy(&ba, &bb);
			assert(bit_array_cmp(&bb, &bc, &cmp) && cmp==0);
			bit_array_cmp_count(&bb, &bc, &o1, &o2, &both);
			assert(o1==0 && o2==0);

			bit_array_random(&bb);
			bit_array_random(&bc);
			bit_array_reset(&ba, 0);
			bit_array_plus_diff(&ba, &bb, &bc); // a=a+(b-c)=b-c
			bit_array_reset(&bd, 0);
			bit_array_plus_diff(&bb, &ba, &bd); // d=d+(b-c)=c-b

			bit_array_copy(&be, &bb);
			bit_array_bwand(&be, &bc); // e=inter(b,c)


			bit_array_copy(&bf, &be);
			bit_array_bwor(&bf, &ba); // bf?=bb
			assert(bit_array_cmp(&bf, &bb, &cmp) && cmp==0);

			bit_array_copy(&bf, &bd);
			bit_array_bwor(&bf, &be); // bf?=bc
			bit_array_cmp_count(&bf, &bc, &o1, &o2, &both);
			assert(o1==0 && o2==0);
        }
        bit_array_release(&ba);
        bit_array_release(&bb);
        bit_array_release(&bc);
        bit_array_release(&bd);
        bit_array_release(&be);
        bit_array_release(&bf);
    }


    if (0)
    {
    	// test zone
        bit_array_init(&ba, test_size, 0);

        bit_array_reset(&ba, 1);
        bit_array_reset_zone(&ba, 0, test_size-1, 0);
        bit_array_print(&ba);

        for (i=0; i<test_size/4; i++)
        {
            bit_array_reset(&ba, 0);
            bit_array_reset_zone(&ba, test_size/3-i, test_size/3+i, 1);
            bit_array_print(&ba);
        }
        printf("\n");

        bit_array_reset(&ba, 0);
        bit_array_reset_zone(&ba, 0, test_size-1, 1);
        bit_array_print(&ba);

        for (i=0; i<test_size/4; i++)
        {
            bit_array_reset(&ba, 1);
            bit_array_reset_zone(&ba, 2*test_size/3-i, 2*test_size/3+1+i, 0);
            bit_array_print(&ba);
        }
        bit_array_release(&ba);

    }

    if (0)
    {
    	printf("test first and n_set basic\n");
        bit_array_init(&ba, test_size, 0);
        bit_array_print(&ba);
        bit_array_reset(&ba, 1);
        bit_array_print(&ba);

        long long int i=-1;
        long long int j=-1;
        while (1)
        {
        	i=bit_array_search_first_set(&ba, i+1);
        	j=bit_array_search_nth_set(&ba, j+1, 0);
        	printf("%lld %lld\n", i, j);
        	if (i==-1) break;
        }
        bit_array_release(&ba);


    }

    if (1)
    {
    	printf("=== test n_set ===\n");
    	int size=61;
        bit_array_init(&ba, size, 0);

        for (i=0; i<10000; i++)
        {
        	bit_array_random(&ba);
        	int i;
        	for (i=0; i<10; i++)
        	{
        		int a=size;
        		int b=size;
        		while (a+b>size)
        		{
        			a=random()%size;
        			b=random()%size+1;
        		}
        		// using bit_array_search_first_set
        		int idx=bit_array_search_first_set(&ba, a);
        		int n1=b-1;
        		while (n1>0 && idx>=0)
        		{
        			idx=bit_array_search_first_set(&ba, idx+1);
        			if (idx>=0) n1--;
        		}

        		int nth=bit_array_search_nth_set(&ba, a, b);
        		int count=-999;
        		if (nth>=0) count=bit_array_count_zone(&ba, a, nth);

        		if (nth!=idx || (nth!=-1 && b!=count))
        		{
        			printf("ERR  set  from=%2d n=%2d count=%2d first=%2d nth=%2d\t", a, b, count, idx, nth);
                    bit_array_print(&ba);
        		}

        		// using bit_array_search_first_set
        		idx=bit_array_search_first_unset(&ba, a);
        		n1=b-1;
        		while (n1>0 && idx>=0)
        		{
        			idx=bit_array_search_first_unset(&ba, idx+1);
        			if (idx>=0) n1--;
        		}

        		nth=bit_array_search_nth_unset(&ba, a, b);
        		if (nth!=idx)
        		{
        			printf("ERR unset from=%2d n=%2d first=%2d nth=%2d ", a, b, idx, nth);
                    bit_array_print(&ba);
        		}

        	}
        }

        bit_array_release(&ba);
    }

    if (1)
    {
    	printf("quick functions\n");
    	long long int capacity=10*1000;
    	if (bit_array_init(&ba, capacity, 0))
    	{
    		perror("malloc bit array\n");
    		return 1;
    	}

        long long int addr;
        long long int used_block;
        int runmax=20;
        for (i=0; i<runmax; i++)
        {
			struct quick_bit_array q_count_zone, q_nth_unset;
			bit_array_random(&ba);
			for (j=0; j<20; j++)
			{
				b=a=0;
				while (b-a<200)
				{
					a=random()%capacity;
					b=random()%capacity;
					if (a>b) { int t=a; a=b; b=t; }
				}

				long long int block_count=b-a+1;
				int quick_size=(b-a)/100;
				used_block=bit_array_count_zone(&ba, a, b);
				fprintf(stderr, "random set %d/%d, used: %8lld/%-8lld a=%-8lld b=%-8lld\r", i, runmax, used_block, block_count, a, b);
				if (quick_bit_array_init(&q_count_zone, &ba, quick_size, a, b, qba_count_zone) ||
					quick_bit_array_init(&q_nth_unset, &ba, quick_size, a, b, qba_nth_unset))
				{
					perror("malloc quick structure");
					exit(1);
				}
				for (addr=a; addr<=b; addr++)
				{
					//assert(quick_bit_array_count_zone(&q_count_zone, addr)==bit_array_count_zone(&ba, a, addr));
					if (quick_bit_array_count_zone(&q_count_zone, addr)!=bit_array_count_zone(&ba, a, addr))
					{
						fprintf(stderr, "addr=%8lld quick=%8lld count=%8lld\n", addr, quick_bit_array_count_zone(&q_count_zone, addr), bit_array_count_zone(&ba, a, addr));
						assert("BAD ADDRESS"==NULL);
					}
				}
				long long int count;
				for (count=0; count<=block_count-used_block; count++)
				{
					// assert(quick_bit_array_search_nth_unset(&q_nth_unset, count)==bit_array_search_nth_unset(&ba, a, count));
					if (quick_bit_array_search_nth_unset(&q_nth_unset, count)!=bit_array_search_nth_unset(&ba, a, count))
					{
						fprintf(stderr, "count=%8lld quick=%8lld nth=%8lld\n", count, quick_bit_array_search_nth_unset(&q_nth_unset, count), bit_array_search_nth_unset(&ba, a, count));
						assert("BAD COUNT"==NULL);
					}
				}

	//			long long int count=addr-a+1-quick_bit_array_count_zone(&q_count_zone, addr);
	//			long long int baddr=quick_bit_array_search_nth_unset(&q_nth_unset, count);
	//			if (addr!=baddr)
	//				{
	//					fprintf(stderr, "addr=%8lld baddr=%8lld delta=%2lld count=%8lld ? %8lld ==> %8lld\n", addr, baddr, baddr-addr, count, addr-a+1-bit_array_count_zone(&ba, a, addr), bit_array_search_nth_unset(&ba, a, count));
	//				}
	//
	//			}
			}
			printf("quick functions SUCCESS\n");
			quick_bit_array_release(&q_count_zone);
			quick_bit_array_release(&q_nth_unset);
        }
        bit_array_release(&ba);

    }

    if (1)
    {
    	printf("ddumbfs performance test\n");
    	long long int block_count=400LL*1000*1000;
    	int quick_size=5*1000*1000;
// Time
//    	bc*1E6	400	800	1600
//    	quick*1E6
//    	0.625	285	898	3130
//    	1.25	205	573	1810
//    	2.5		163	409	1140
//    	5		147	323	808 <- optimum is between 5 and 10 for the 3 curve
//    	10		130	281	650
//    	20		127	262	564
//    	40		123	251	526
// "optimum size vs time " => quick_size=block_count/400
    	if (bit_array_init(&ba, block_count, 0) || bit_array_init(&bb, block_count, 0))
    	{
    		perror("malloc bit array\n");
    		return 1;
    	}

        long long int addr;
        long long int used_block, _u;
        bit_array_random(&ba);
        bit_array_copy(&ba, &bb);
        bit_array_count(&ba, &used_block, &_u);
        fprintf(stderr, "random set, used: %lld/%lld\n", used_block, block_count);

        start=last=now();
        struct quick_bit_array q_count_zone, q_nth_unset;
        if (quick_bit_array_init(&q_count_zone, &ba, quick_size, used_block, block_count, qba_count_zone) ||
            quick_bit_array_init(&q_nth_unset, &ba, quick_size, 0, used_block, qba_nth_unset))
        {
        	perror("malloc quick structure");
        	exit(1);
        }
        end=now();
        fprintf(stderr, "quick initialized in %.1fs\n", (end-start)/1000.0);
        start=now();
        for (addr=used_block; addr<block_count; addr++)
        {
        	long long int count=quick_bit_array_count_zone(&q_count_zone, addr);
        	long long int baddr=quick_bit_array_search_nth_unset(&q_nth_unset, count);

        	bit_array_unset(&bb, addr);
        	bit_array_set(&bb, baddr);

        	end=now();
            if (end-last>1000)
            {
                last=end;
                fprintf(stderr, "calculate %4.1f%% in %llds --> %.1fs\r", (addr-used_block)*100.0/(block_count-used_block), (end-start)/1000, (end-start)/1000.0/(addr-used_block)*(block_count-used_block));
            }

        }
        end=now();
        fprintf(stderr, "remap2 in %.1fs                 \n", (end-start)/1000.0);

    	assert(0==bit_array_search_first_set(&bb, 0));
    	assert(used_block==bit_array_search_first_unset(&bb, 0));
    	assert(-1==bit_array_search_first_set(&bb, used_block));
    	assert(used_block==bit_array_count_zone(&bb, 0, used_block-1));
    	assert(0==bit_array_count_zone(&bb, used_block, bb.size-1));
    	assert(used_block==bit_array_count_zone(&bb, 0, bb.size-1));
        quick_bit_array_release(&q_count_zone);
        quick_bit_array_release(&q_nth_unset);
        bit_array_release(&bb);
        bit_array_release(&ba);

    }


    if (0)
    {
    	printf("=== test bit_array_search_nth_set === \n");
    	int size=61;
        bit_array_init(&ba, size, 0);
        bit_array_print(&ba);
        bit_array_set(&ba, 1);
        bit_array_set(&ba, 58);
        bit_array_set(&ba, 4);
        bit_array_set(&ba, 55);
        bit_array_set(&ba, 15);
        bit_array_print(&ba);
        printf("search first set from 0 : %lld\n", bit_array_search_first_set(&ba, 0));
        printf("search first set from 1 : %lld\n", bit_array_search_first_set(&ba, 1));
        printf("search first set from 2 : %lld\n", bit_array_search_first_set(&ba, 2));
        printf("search first set from 13 : %lld\n", bit_array_search_first_set(&ba, 13));
        printf("search first set from 15 : %lld\n", bit_array_search_first_set(&ba, 15));
        printf("search first set from 16 : %lld\n", bit_array_search_first_set(&ba, 16));
        printf("search first set from 55 : %lld\n", bit_array_search_first_set(&ba, 55));
        printf("search first set from 56 : %lld\n", bit_array_search_first_set(&ba, 56));
        printf("search first set from 59 : %lld\n", bit_array_search_first_set(&ba, 59));

        printf("search 2nd set from 0 : %lld\n", bit_array_search_nth_set(&ba, 0, 1));
        printf("search 2nd set from 1 : %lld\n", bit_array_search_nth_set(&ba, 1, 1));
        printf("search 2nd set from 2 : %lld\n", bit_array_search_nth_set(&ba, 2, 1));
        printf("search 2nd set from 13 : %lld\n", bit_array_search_nth_set(&ba, 13, 1));
        printf("search 2nd set from 15 : %lld\n", bit_array_search_nth_set(&ba, 15, 1));
        printf("search 2nd set from 16 : %lld\n", bit_array_search_nth_set(&ba, 16, 1));
        printf("search 2nd set from 56 : %lld\n", bit_array_search_nth_set(&ba, 56, 1));
        printf("search 2nd set from 59 : %lld\n", bit_array_search_nth_set(&ba, 59, 1));

        bit_array_inverse_into(&ba, &bb);
        bit_array_print(&bb);
        printf("search first unset from 0 : %lld\n", bit_array_search_first_unset(&bb, 0));
        printf("search first unset from 1 : %lld\n", bit_array_search_first_unset(&bb, 1));
        printf("search first unset from 2 : %lld\n", bit_array_search_first_unset(&bb, 2));
        printf("search first unset from 15 : %lld\n", bit_array_search_first_unset(&bb, 15));
        printf("search first unset from 55 : %lld\n", bit_array_search_first_unset(&bb, 55));
        printf("search first unset from 56 : %lld\n", bit_array_search_first_unset(&bb, 56));
        printf("search first unset from 59 : %lld\n", bit_array_search_first_unset(&bb, 59));

        bit_array_unset(&ba, 1);
        bit_array_unset(&ba, 58);
        bit_array_print(&ba);

        long long int s, u;
        unsigned char buf[1024];

        bit_array_save(&ba, "/tmp/bit_array", 1);
        for (i=0; i<ba.isize*BIT_INT_BYTE; i++) printf("%02X", (int)buf[i]);
        printf("\n");
        bit_array_save(&bb, "/tmp/bit_array", 1);
        bit_array_print(&bb);
        bit_array_count(&bb, &s, &u);
        printf("count %lld + %lld = %lld\n", s, u, s+u);
        for (i=0; i<test_size-2; i++) printf("%lld ", bit_array_alloc(&bb));
        bit_array_count(&bb, &s, &u);
        printf("\ncount %lld + %lld = %lld\n", s, u, s+u);

        bit_array_release(&ba);
        bit_array_release(&bb);
    }

    if (0)
    {
        struct bit_array big;
        long long int n=2097152;
        bit_array_init(&big, n, 0);
        long long int i;

        printf("init\n");
        clock_t start, end;

        start=clock();
        bit_array_set(&big, n/3);
        bit_array_set(&big, 2*n/3);
        long long int p1, p2;
        for (i=0; i<10000; i++)
        {
            p1=bit_array_search_first_set(&big, 0);
            p2=bit_array_search_first_set(&big, p1+1);
        }
        end=clock();
        printf("search first set from 0    : %lld\n", p1);
        printf("search first set from prev : %lld\n", p2);

        printf("%lld\n",(long long int)(end-start));
        bit_array_release(&big);

    }
    return 0;

}
