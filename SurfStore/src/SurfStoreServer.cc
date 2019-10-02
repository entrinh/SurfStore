#include <sysexits.h>
#include <string>

#include "rpc/server.h"
#include "rpc/this_handler.h"

#include "logger.hpp"
#include "SurfStoreTypes.hpp"
#include "SurfStoreServer.hpp"

SurfStoreServer::SurfStoreServer(INIReader &t_config)
	: config(t_config)
{
	auto log = logger();

	// pull the address and port for the server
	string servconf = config.Get("ssd", "server", "");
	if (servconf == "")
	{
		log->error("Server line not found in config file");
		exit(EX_CONFIG);
	}
	size_t idx = servconf.find(":");
	if (idx == string::npos)
	{
		log->error("Config line {} is invalid", servconf);
		exit(EX_CONFIG);
	}
	port = strtol(servconf.substr(idx + 1).c_str(), nullptr, 0);
	if (port <= 0 || port > 65535)
	{
		log->error("The port provided is invalid: {}", servconf);
		exit(EX_CONFIG);
	}
}

void SurfStoreServer::launch()
{
	auto log = logger();

	log->info("Launching SurfStore server");
	log->info("Port: {}", port);

	rpc::server srv(port);

	srv.bind("ping", []() {
		auto log = logger();
		log->info("Received ping request");
		return;
	});

	srv.bind("get_block", [this](string hash) {
		auto log = logger();
		log->info("Received get_block request");
		return blockStore[hash];
	});

	srv.bind("store_block", [this](string hash, string data) {
		auto log = logger();
		log->info("Received store_block request");
		log->info("Hash: {}\n", hash);
		blockStore[hash] = data;
		return;
	});

	srv.bind("get_fileinfo_map", [this]() {
		auto log = logger();
		log->info("Received get_fileinfo_map request");
		log->info("");
		log->info("Server Map:");
		for (auto i : fmap)
		{
			log->info("{} : {} : {}", i.first, get<0>(i.second), get<1>(i.second).front());
		}
		log->info("");
		return fmap;
	});

	//TODO: update the FileInfo entry for a given file
	srv.bind("update_file", [this](string filename, FileInfo finfo) {
		auto log = logger();
		log->info("Received update_file request");
		log->info("Filename: {}", filename);
		log->info("File Version: {}", get<0>(finfo));
		log->info("Server Version: {}", get<0>(fmap[filename]));
		if (get<0>(finfo) > get<0>(fmap[filename]))
		{
			FileInfo servFile = fmap[filename];
			if (get<0>(finfo) == 1 || get<0>(finfo) == (get<0>(servFile) + 1))
			{
				//update finfo
				fmap[filename] = finfo;
			}
			log->info("");
			log->info("Server Map:");
			for (auto i : fmap)
			{
				log->info("{} : {} : {}", i.first, get<0>(i.second), get<1>(i.second).front());
			}
			log->info("");
		}
		else
		{
			rpc::this_handler().respond_error("The update is not valid");
		}
	});

	// You may add additional RPC bindings as necessary

	srv.run();
}
