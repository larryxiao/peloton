#include <stdio.h>

class foo{
    union operation {
        // Search the leaf page PID for the given key
        int (foo::*f1)(int a);
        int (foo::*f2)(int a, int b);
    };
public:
    operation op;
    int (foo::*f3)(int a);
    int bar(int number){
        return (number*10);
    }
    foo() {
        op.f1 = &foo::bar;
        f3 = &foo::bar;
    }
    int operate(int a) {
        //int ret = .f1(a);
        return a;
    }
};

int (*f3)(int a);
int baz(int a) {
    return a * a;
}

int main(int argc, const char *argv[])
{
    foo f;
    f3 = baz;
    int (foo::*fptr) (int) = &foo::bar;
    int ret1 = f3(5);
    int ret2 = (f.*fptr)(5);
    foo* p=&f;
    int ret3 = (p->*p->f3)(5);
    int ret4 = (f.*f.f3)(5);
    int ret5 = (f.*f.op.f1)(5);
    int ret6 = (p->*p->op.f1)(5);
    printf("%d %d %d %d %d %d\n", ret1, ret2, ret3, ret4, ret5, ret6);
    return 0;
}   
