#ifndef SURFSTORESERVER_HPP
#define SURFSTORESERVER_HPP

#include "inih/INIReader.h"
#include "logger.hpp"

using namespace std;
typedef tuple<int, list<string>> FileInfo;
typedef map<string, FileInfo> FileInfoMap;


class SurfStoreServer {
public:
    SurfStoreServer(INIReader& t_config);

    void launch();

	const int NUM_THREADS = 8;

protected:
    INIReader& config;
	int port;
    map<string,string> blockStore;
    FileInfoMap fmap;
};

#endif // SURFSTORESERVER_HPP
