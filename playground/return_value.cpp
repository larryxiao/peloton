//https://en.wikipedia.org/wiki/Return_value_optimization
#include <iostream>
#include <stdio.h>

struct C {
    int _c;
    C(int c) { _c = c; std::cout << "Construct.\n"; }
    C(const C&) { std::cout << "Copy.\n"; }
};

C f() {
    struct C c(1);
    int d;
    printf("%p %p\n", &c, &d);
    return c;
}

int main() {
    std::cout << "Hello World!\n";
    C obj = f();
    printf("%p\n", &obj);
    std::cout << obj._c;
    return 0;
}
