#include <vector>
#include <iostream>
#include <fstream>
#include "picosha2/picosha2.h"

using namespace std;

int main(int argc, char const *argv[])
{
  int blocksize = 4096;
  ifstream ifile("./ss-clientdir/test1.txt", ifstream::binary);
  char *buffer = new char[blocksize];
  int bytesRead = 0;
  string key;
  vector<string> hashes;
  do
  {
    ifile.read(buffer, blocksize);
    cout << "Bytes Read: " << bytesRead << endl;
    cout << "Buffer: " << buffer[0] << endl;
    key = picosha2::hash256_hex_string(string(buffer, bytesRead));
    cout << key << endl;
    hashes.push_back(key);
  } while (!ifile.eof());
  delete[] buffer;

  return 0;
}
