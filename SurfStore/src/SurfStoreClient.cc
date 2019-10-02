#include <sysexits.h>
#include <sys/stat.h>
#include <string>
#include <vector>
#include <algorithm>
#include <fstream>
#include <map>
#include <iostream>
#include <sstream>
#include <fstream>
#include <string>

#include <sys/types.h>
#include <dirent.h>
#include <assert.h>
#include <stdio.h>

#include "rpc/server.h"
#include "picosha2/picosha2.h"

#include "logger.hpp"
#include "SurfStoreTypes.hpp"
#include "SurfStoreClient.hpp"

using namespace std;

SurfStoreClient::SurfStoreClient(INIReader &t_config)
    : config(t_config), c(nullptr)
{
  auto log = logger();

  // pull the address and port for the server
  string serverconf = config.Get("ssd", "server", "");
  if (serverconf == "")
  {
    log->error("The server was not found in the config file");
    exit(EX_CONFIG);
  }
  size_t idx = serverconf.find(":");
  if (idx == string::npos)
  {
    log->error("Config line {} is invalid", serverconf);
    exit(EX_CONFIG);
  }
  serverhost = serverconf.substr(0, idx);
  serverport = strtol(serverconf.substr(idx + 1).c_str(), nullptr, 0);
  if (serverport <= 0 || serverport > 65535)
  {
    log->error("The port provided is invalid: {}", serverconf);
    exit(EX_CONFIG);
  }

  base_dir = config.Get("ss", "base_dir", "");
  blocksize = config.GetInteger("ss", "blocksize", 4096);

  log->info("Launching SurfStore client");
  log->info("Server host: {}", serverhost);
  log->info("Server port: {}", serverport);

  c = new rpc::client(serverhost, serverport);
}

SurfStoreClient::~SurfStoreClient()
{
  if (c)
  {
    delete c;
    c = nullptr;
  }
}

void SurfStoreClient::sync()
{
  auto log = logger();

  c->call("ping");
  vector<pair<string, vector<string>>> baseDir;
  FileInfoMap files;
  vector<string> toUpload, toDownload, toDelete;

  //get files from baseDir
  DIR *dirp = opendir(base_dir.c_str());
  struct dirent *dp;
  while ((dp = readdir(dirp)) != NULL)
  {

    //compute hashcode for each file
    if (dp->d_name[0] != '.' && string(dp->d_name) != "index.txt")
    {
      log->info("Found file: {}", string(dp->d_name));
      baseDir.push_back(make_pair(string(dp->d_name), getHash(base_dir + "/" + string(dp->d_name))));
    }
  }
  closedir(dirp);

  //check hashcode against index.txt
  for (auto file : baseDir) // go through all files in baseDir
  {
    FileInfo indInfo = get_local_fileinfo(file.first);
    vector<string> indHashes{(get<1>(indInfo)).begin(), (get<1>(indInfo)).end()};
    list<string> hashList((file.second).begin(), (file.second).end());
    files[file.first] = make_tuple(get<0>(indInfo), hashList);

    if (get<0>(indInfo) != -1 && indHashes.size() == file.second.size())
    {
      for (unsigned long i = 0; i < indHashes.size(); i++)
      {
        log->info("Index Value: {}\tFile Value: {}", indHashes[i], file.second[i]);
        if (indHashes[i] != file.second[i])
        {
          toUpload.push_back(file.first);
          break;
        }
      }
    }
    else
    {
      log->info("Size is Different, or Does not exist in index.txt");
      toUpload.push_back(file.first);
    }
  }

  //update files with server
  //get updated FileInfoMap
  log->info("Retrieving Server Map");
  FileInfoMap serverMap = c->call("get_fileinfo_map").as<FileInfoMap>();
  log->info("Filename : Version : Hash[0]");
  for (auto i : serverMap)
  {
    log->info("{} : {} : {}", i.first, get<0>(i.second), get<1>(i.second).front());
  }
  log->info("\n");

  //check for deleted files
  ifstream f(base_dir + "/index.txt");
  if (!f.fail())
  {
    log->info("Checking for deleted file");
    do
    {
      vector<string> parts;
      string x;
      getline(f, x);
      stringstream ss(x);
      string tok;
      while (getline(ss, tok, ' '))
      {
        parts.push_back(tok);
      }
      if (parts.size() > 0 && files.find(parts[0]) == files.end() && parts[2] != "0")
      {
        FileInfo delInfo;
        log->info("{} {} {}", parts[0], parts[1], parts[2]);
        int v = stoi(parts[1]);
        get<0>(delInfo) = v;
        get<1>(delInfo) = {"0"};
        files[parts[0]] = delInfo;
        toUpload.push_back(parts[0]);
      }
    } while (!f.eof());
  }

  //refine what files need to be uploaded
  for (auto file : files)
  {
    log->info("File: {}", file.first);
    if (serverMap.find(file.first) == serverMap.end() && find(toUpload.begin(), toUpload.end(), file.first) == toUpload.end())
    {
      toUpload.push_back(file.first);
    }
  }
  for (auto ufile : toUpload)
  {
    log->info("Current ufile: {}", ufile);
    int locVer = get<0>(files[ufile]) + 1;
    if (locVer == 0)
      locVer++;
    if (serverMap.find(ufile) != serverMap.end())
    {
      int servVer = get<0>(serverMap[ufile]);
      log->info("{}'s Local version: {}\tServer version: {}", ufile, locVer, servVer);
      if (locVer != servVer + 1)
      {
        toUpload.push_back(ufile);
        log->info("{} removed from toUpload", ufile);
      }
    }
  }

  //check what files need to be downloaded
  log->info("Checking ServerMap");
  for (auto dfile : serverMap)
  {
    if (find(toUpload.begin(), toUpload.end(), dfile.first) != toUpload.end())
      continue;
    int servVer = get<0>(dfile.second);
    log->info("{} : {} : {}", dfile.first, get<0>(dfile.second), get<1>(dfile.second).front());
    if (files.find(dfile.first) != files.end())
    {
      int locVer = get<0>(files[dfile.first]);
      log->info("{}'s Local version: {}\tServer version: {}", dfile.first, locVer, servVer);
      if (servVer > locVer)
      {
        toDownload.push_back(dfile.first);
        log->info("Need to Download: {}", dfile.first);
      }
    }
    else
    {
      toDownload.push_back(dfile.first);
    }
  }

  //Downloading new files
  ofstream downloadFile;
  string hash;
  for (string dfile : toDownload)
  {
    log->info("Current file to download: {}", dfile);
    downloadFile.open(base_dir + "/" + dfile);
    list<string> hashList = get<1>(serverMap[dfile]);
    log->info("Number of Hashes: {}", hashList.size());
    if (hashList.front() == "0")
    {
      downloadFile.close();
      string filepath = base_dir + "/" + dfile;
      std::remove(filepath.c_str());
    }
    else
    {
      for (auto itr = hashList.begin(); itr != hashList.end(); itr++)
      {
        downloadFile << (c->call("get_block", *itr).as<string>());
      }
      downloadFile.close();
    }
    set_local_fileinfo(dfile, serverMap[dfile]);
  }

  //Uploading new files
  for (string ufile : toUpload)
  {
    log->info("Current file to upload: {}", ufile);
    FileInfo uinfo = files[ufile];
    FileInfo newInfo;
    log->info("Local Version: {}", get<0>(uinfo));
    if (get<0>(uinfo) == -1)
    {
      newInfo = make_tuple(1, get<1>(uinfo));
    }
    else if (serverMap.find(ufile) == serverMap.end())
    {
      newInfo = make_tuple(get<0>(uinfo), get<1>(uinfo));
    }
    else
    {
      newInfo = make_tuple(get<0>(uinfo) + 1, get<1>(uinfo));
    }
    storeBlock(base_dir + "/" + ufile);
    try
    {
      c->call("update_file", ufile, newInfo);
    }
    catch (exception e)
    {
      log->info("The update out of Date downloading new file");
      log->info("Current file to download: {}", ufile);
      downloadFile.open(base_dir + "/" + ufile);
      list<string> hashList = get<1>(serverMap[ufile]);
      log->info("Number of Hashes: {}", hashList.size());

      for (auto itr = hashList.begin(); itr != hashList.end(); itr++)
      {
        downloadFile << (c->call("get_block", *itr).as<string>());
      }
      downloadFile.close();

      // set_local_fileinfo(ufile, serverMap[ufile]);
    }
    set_local_fileinfo(ufile, newInfo);
  }
}

vector<string> SurfStoreClient::getHash(string filepath)
{
  auto log = logger();
  log->info("Getting Hash for {}", filepath);
  ifstream ifile(filepath, ios::in | ios::binary);
  char *buffer = new char[blocksize];
  string key;
  vector<string> hashes;
  ifile.seekg(0, ifile.end);
  int length = ifile.tellg();
  ifile.seekg(0, ifile.beg);
  log->info("Length: {}", length);
  while (length > 0)
  {
    ifile.read(buffer, blocksize);
    if (length >= blocksize)
      key = picosha2::hash256_hex_string(string(buffer, blocksize));
    else
      key = picosha2::hash256_hex_string(string(buffer, length));
    log->info("Key: {}", key);
    hashes.push_back(key);
    length -= blocksize;
  }
  log->info('\n');
  delete[] buffer;
  return hashes;
}

void SurfStoreClient::storeBlock(string filepath)
{
  auto log = logger();
  ifstream ifile(filepath, ios::in | ios::binary);
  char *buffer = new char[blocksize];
  string hash, data;
  vector<string> hashes;
  ifile.seekg(0, ifile.end);
  int length = ifile.tellg();
  log->info("Length of {}: {}", filepath, length);
  ifile.seekg(0, ifile.beg);
  while (length > 0)
  {
    ifile.read(buffer, blocksize);
    if (length >= blocksize)
      data = string(buffer, blocksize);
    else
      data = string(buffer, length);
    hash = picosha2::hash256_hex_string(data);
    c->call("store_block", hash, data);
    length -= blocksize;
  }
  delete[] buffer;
  return;
}

FileInfo SurfStoreClient::get_local_fileinfo(string filename)
{
  auto log = logger();
  log->info("get_local_fileinfo {}", filename);
  ifstream f(base_dir + "/index.txt");
  if (f.fail())
  {
    int v = -1;
    list<string> blocklist;
    FileInfo ret = make_tuple(v, list<string>());
    return ret;
  }
  do
  {
    vector<string> parts;
    string x;
    getline(f, x);
    stringstream ss(x);
    string tok;
    while (getline(ss, tok, ' '))
    {
      parts.push_back(tok);
    }
    if (parts.size() > 0 && parts[0] == filename)
    {
      list<string> hl(parts.begin() + 2, parts.end());
      int v = stoi(parts[1]);
      return make_tuple(v, hl);
    }
  } while (!f.eof());
  int v = -1;
  list<string> blocklist;
  FileInfo ret = make_tuple(v, list<string>());
  return ret;
}

void SurfStoreClient::set_local_fileinfo(string filename, FileInfo finfo)
{
  auto log = logger();
  log->info("set local file info");
  std::ifstream f(base_dir + "/index.txt");
  std::ofstream out(base_dir + "/index.txt.new");
  int v = get<0>(finfo);
  list<string> hl = get<1>(finfo);
  bool set = false;
  do
  {
    string x;
    vector<string> parts;
    getline(f, x);
    stringstream ss(x);
    string tok;
    while (getline(ss, tok, ' '))
    {
      parts.push_back(tok);
    }
    if (parts.size() > 0)
    {
      if (parts[0] == filename)
      {
        set = true;
        out << filename << " " << v << " ";
        for (auto it : hl)
          out << it << " ";
        out.seekp(-1, ios_base::cur);
        out << "\n";
      }
      else
      {
        out << x << "\n";
      }
    }
    else
      break;
  } while (!f.eof());
  if (!set)
  {
    out << filename << " " << v << " ";
    for (auto it : hl)
      out << it << " ";
    out.seekp(-1, ios_base::cur);
    out << "\n";
  }
  out.close();
  f.close();
  string real = string(base_dir + "/index.txt");
  string bkp = string(base_dir + "/index.txt.new");

  remove(real.c_str());
  rename(bkp.c_str(), real.c_str());
}
