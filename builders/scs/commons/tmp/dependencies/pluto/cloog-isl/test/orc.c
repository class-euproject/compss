/* Generated from ./orc.cloog by CLooG 0.18.1-2-g43fc508 gmp bits in 0.01s. */
S1(0);
S2(0,0);
for (p2=1; p2<=22; p2++) {
    if ((p2+1)%2 == 0) {
        S3(0,((p2-1)/2));
    }
    if (p2%2 == 0) {
        S2(0,(p2/2));
    }
}
S3(0,11);
for (p1=2; p1<=6; p1++) {
    if ((p1+1)%3 == 0) {
        S4(((p1-2)/3));
    }
    if (p1%3 == 0) {
        S1((p1/3));
    }
    if (p1 == 4) {
        S2(1,0);
    }
    if (p1 == 4) {
        for (p2=1; p2<=20; p2++) {
            if ((p2+1)%2 == 0) {
                S3(1,((p2-1)/2));
            }
            if (p2%2 == 0) {
                S2(1,(p2/2));
            }
        }
    }
    if (p1 == 4) {
        S3(1,10);
    }
}
S2(2,0);
for (p2=1; p2<=18; p2++) {
    if ((p2+1)%2 == 0) {
        S3(2,((p2-1)/2));
    }
    if (p2%2 == 0) {
        S2(2,(p2/2));
    }
}
S3(2,9);
S4(2);
S5(0);
for (p2=0; p2<=9; p2++) {
    S6(0,p2);
}
for (p1=2; p1<=42; p1++) {
    if ((p1+1)%3 == 0) {
        S7(((p1-2)/3));
    }
    if (p1%3 == 0) {
        S5((p1/3));
    }
    for (p2=0; p2<=9; p2++) {
        if ((p1+2)%3 == 0) {
            S6(((p1-1)/3),p2);
        }
    }
}
for (p2=0; p2<=9; p2++) {
    S6(14,p2);
}
S7(14);
