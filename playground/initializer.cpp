/*********************************************************************************
*     File Name           :     initializer.cpp
*     Created By          :     xiaodi
*     Creation Date       :     [2016-02-20 15:50]
*     Last Modified       :     [2016-02-20 15:59]
*     Description         :      
**********************************************************************************/

#include <atomic>
#include <vector>

//-std=c++14

using namespace std;

struct {
    int i;
} Node;

int main(int argc, const char *argv[])
{
    vector<atomic<struct Node *>> t1;
    vector<struct Node *> t2{100};
    vector<int> t3;
    vector<int> t4 = {100};
    return 0;
}


