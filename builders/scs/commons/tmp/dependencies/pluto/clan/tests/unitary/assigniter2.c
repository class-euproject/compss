#pragma scop
for (i = 0; i < N; ++i)
    for (j = 0; j < N; ++j)
        A[i][j] = (i * j) / N;
#pragma endscop
