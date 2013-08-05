#include <string> 
#include "data_entry.hpp"
#include "key_value_pack.hpp"
#include "tair_client_api.hpp"

using namespace std;
using namespace tair;

const static int area = 0;
const static int loop_times = 10;
const static int MAGIC_NUM = 30000;
void base_api_test(tair_client_api *client_helper);
void prefix_api_test(tair_client_api *client_helper);
void count_api_test(tair_client_api *client_helper);
void range_api_test(tair_client_api *client_helper);

int main()
{
  const char *cs1 = "xx.xxx.xxx.xx:5198";
  const char *cs2 = "xx.xxx.xxx.xx:5198";
  const char *group = "group_name";

  tair_client_api *client_helper = new tair_client_api();
  if ( !client_helper->startup(cs1, cs2, group) ){
    delete client_helper;
    client_helper = NULL;
    cout << "startup failed.\n";
    exit(-1);
  }

  base_api_test(client_helper);
  prefix_api_test(client_helper);
  count_api_test(client_helper);
  range_api_test(client_helper);

  client_helper->close();
  delete client_helper;
  client_helper = NULL;
  return 0;
}

template<class T> void delete_vec(T* vec)
{
 typename T::iterator iter = vec->begin();
 for (; iter != vec->end(); iter++)
 {
   if (NULL != (*iter))
   {
     delete (*iter);
     (*iter) = NULL;
   }
 }
 vec->clear();
}

void base_api_test(tair_client_api *client_helper)
{
  const char* ckey = "key1";
  const char* cvalue = "value1";
  data_entry key(ckey, strlen(ckey) + 1, true); //we set size to strlen()+1 to store extra '\0',so it can be printfed safely as expected when it is humen-readable string.
  data_entry value(cvalue, strlen(cvalue) + 1, true); //ditto
  data_entry* ret_value;
  int expire = 60;
  int version = 0;
  int ret = TAIR_RETURN_SUCCESS;

  //put/get/remove
  if (TAIR_RETURN_SUCCESS != (ret = client_helper->put(area, key, value, expire, version)))
  {
    cout << "put failed. key: " << key.get_data() << ", value: " << value.get_data() << ", ret: " << ret << endl;
  }
  else
  {
    if (TAIR_RETURN_SUCCESS != (ret = client_helper->get(area, key, ret_value)))
    {
      cout << "get failed. key: " << key.get_data() << ", ret: " << ret << endl;
    }
    else
    {
      cout << "get succ. key: " << key.get_data() << ", value: " << ret_value->get_data() << ", ret: " << ret << endl;
      delete ret_value; 
      ret_value = NULL;
      ret = client_helper->remove(area, key);
      cout << "remove end. key: " << key.get_data() << ", ret: " << ret << endl;
    }
  }

  //put/mget/mdelete
  tair_dataentry_vector mkeys;
  tair_dataentry_vector mvalues;
  tair_keyvalue_map mkvs;

  for (int i = 0; i < loop_times; i++)
  {
    char buf[16];
    sprintf(buf, "%s_%d", "key", i);
    data_entry* tmp_key = new data_entry(buf); // it will alloc data by default
    sprintf(buf, "%s_%d", "value", i);
    data_entry* tmp_value = new data_entry(buf); // it will alloc data by default
    if (TAIR_RETURN_SUCCESS != (ret = client_helper->put(area, *tmp_key, *tmp_value, expire, version)))
    {
      cout << "put failed. key: " << tmp_key->get_data() << ", value: " << tmp_value->get_data() << ", ret: " << ret << endl;
      delete tmp_key;
      tmp_key = NULL;
      delete tmp_value;
      tmp_value = NULL;
    }
    else
    {
      cout << "put succ. key: " << tmp_key->get_data() << ", value: " << tmp_value->get_data() << ", ret: " << ret << endl;
      mkeys.push_back(tmp_key);
      mvalues.push_back(tmp_value);
    }
  }
  char buf[16];
  sprintf(buf, "%s_%s", "key", "noexist");
  data_entry* tmp_key = new data_entry(buf); // it will alloc data by default
  sprintf(buf, "%s_%s", "value", "noexist");
  data_entry* tmp_value = new data_entry(buf); // it will alloc data by default
  mkeys.push_back(tmp_key);
  mvalues.push_back(tmp_value);

  ret = client_helper->mget(area, mkeys, mkvs);
  if ((TAIR_RETURN_SUCCESS != ret) && (TAIR_RETURN_PARTIAL_SUCCESS != ret))
  {
    cout << "mget failed. ret: " << ret << endl;
  }
  else
  {
    cout << "mget succ or partly succ. ret: " << ret << endl;
    cout << "all keys count:" << mkeys.size() << ", succ keys count: " << mkvs.size() << endl;
    int count = mkeys.size();
    for (int i = 0; i < count; i++)
    {
      data_entry* k = mkeys.at(i);
      tair_keyvalue_map::iterator itr = mkvs.find(k); 
      if ( itr != mkvs.end() )
      {
        cout << "no" << i << ": " << itr->first->get_data() << " => " << itr->second->get_data() << endl;
        delete itr->first;
        //itr->first = NULL;
        delete itr->second; 
        itr->second = NULL;
      }
      else
      {
        // you can retry those failed keys, if those keys do not exists, only do retry once.
      }
    }
  }
  mkvs.clear();

  ret = client_helper->mdelete(area, mkeys); //or you can use minvalid instead
  cout << "mdelete end. " << "ret: " << ret << endl;

  delete_vec(&mkeys);
  delete_vec(&mvalues);
}

void prefix_api_test(tair_client_api *client_helper)
{
  data_entry pkey("pkey");
  data_entry skey("skey");
  data_entry value("value");
  data_entry* ret_value;
  int ret = TAIR_RETURN_SUCCESS;

  //prefix_put/prefix_get/prefix_invalidate
  if (TAIR_RETURN_SUCCESS != (ret = client_helper->prefix_put(area, pkey, skey, value, 60, 0)))
  {
    cout << "prefix_put failed. pkey: " << pkey.get_data() << ", skey: " << skey.get_data() << ", value: " << value.get_data() << ", ret: " << ret << endl;
  }
  else
  {
    cout << "prefix_put succ. pkey: " << pkey.get_data() << ", skey: " << skey.get_data() << ", value: " << value.get_data() << ", ret: " << ret << endl;
    if (TAIR_RETURN_SUCCESS != (ret = client_helper->prefix_get(area, pkey, skey, ret_value)))
    {
      cout << "prefix_get failed. pkey: " << pkey.get_data() << ", skey: " << skey.get_data() << ", ret: " << ret << endl;
    }
    else
    {
      cout << "prefix_get succ. pkey: " << pkey.get_data() << ", skey: " << skey.get_data() << ", value: " << ret_value->get_data() << ", ret: " << ret << endl;
      delete ret_value;
      ret_value = NULL;
      if (TAIR_RETURN_SUCCESS != (ret = client_helper->prefix_remove(area, pkey, skey)))
      {
        cout << "prefix_remove failed. pkey: " << pkey.get_data() << ", skey: " << skey.get_data() << ", ret: " << ret << endl;
      }
      else
      {
        cout << "prefix_remove succ. pkey: " << pkey.get_data() << ", skey: " << skey.get_data() << ", ret: " << ret << endl;
      }
    }
  }

  //prefix_puts/prefix_gets/prefix_removes
  vector<key_value_pack_t*> mskvs; 
  key_code_map_t failed_map;
  tair_dataentry_set skey_set;
  tair_keyvalue_map result_map;

  for (int i = 0; i < loop_times; i++)
  {
    char buf[16];
    sprintf(buf, "%s_%d", "key", i);
    data_entry* tmp_key = new data_entry(buf); // it will alloc data by default
    skey_set.insert(tmp_key);

    sprintf(buf, "%s_%d", "value", i);
    data_entry* tmp_value = new data_entry(buf); // it will alloc data by default
    key_value_pack_t* tmp_pack = new key_value_pack_t;
    tmp_pack->key = tmp_key;
    tmp_pack->value = tmp_value;
    tmp_pack->version = 0;
    tmp_pack->expire = 0;
    mskvs.push_back(tmp_pack);
  }
  if (TAIR_RETURN_SUCCESS != (ret = client_helper->prefix_puts(area, pkey, mskvs, failed_map)))
  {
    cout << "prefix_puts failed. pkey: " << pkey.get_data() << ", ret: " << ret << endl; 
    key_code_map_t::iterator itr = failed_map.begin();
    for (; itr != failed_map.end(); itr++)
    {
      cout << "failed skey: " << itr->first->get_data() << ", ret: " << itr->second << endl; 
      //retry ?
      //do what you want
    }
  }
  else
  {
    cout << "prefix_puts succ. pkey: " << pkey.get_data() << ", ret: " << ret << endl; 
    if (TAIR_RETURN_SUCCESS != (ret = client_helper->prefix_gets(area, pkey, skey_set, result_map, failed_map)))
    {
      cout << "prefix_gets failed. pkey: " << pkey.get_data() << ", ret: " << ret << endl; 
    }
    else
    {
      cout << "prefix_gets succ. pkey: " << pkey.get_data() << ", ret: " << ret << endl; 
      if (TAIR_RETURN_SUCCESS != (ret = client_helper->prefix_removes(area, pkey, skey_set, failed_map)))
      {
        cout << "prefix_removes failed. pkey: " << pkey.get_data() << ", ret: " << ret << endl; 
      }
      else
      {
        cout << "prefix_removes succ. pkey: " << pkey.get_data() << ", ret: " << ret << endl; 
      }
    }
  }

  vector<key_value_pack_t*>::iterator kv_itr = mskvs.begin();
  for (; kv_itr != mskvs.end(); kv_itr++)
  {
    delete ((*kv_itr)->key);
    (*kv_itr)->key = NULL;
    delete ((*kv_itr)->value);
    (*kv_itr)->value = NULL;
    delete *kv_itr;
    *kv_itr = NULL;
  }
  mskvs.clear();
  tair_dataentry_set::iterator s_itr = skey_set.begin();
  for (; s_itr != skey_set.end(); s_itr++)
  {
    delete (*s_itr);
  }
  skey_set.clear();
  tair_keyvalue_map::iterator r_itr = result_map.begin();
  for (; r_itr != result_map.end(); r_itr++)
  {
    delete r_itr->first;
    delete r_itr->second;
    r_itr->second = NULL;
  }
  result_map.clear();
  key_code_map_t::iterator f_itr = failed_map.begin();
  for (; f_itr != failed_map.end(); f_itr++)
  {
    delete f_itr->first;
  }
  failed_map.clear();
}

void count_api_test(tair_client_api *client_helper)
{
  data_entry key("cykey");
  int init_count = 3;
  int diff_count = 2;
  int ret_count;
  data_entry* ret_value;
  int ret = TAIR_RETURN_SUCCESS;

  //incr/decr/get/setcount
  // !! Attention: you can not use put client_helper to set default, or return cannot override error.
  //               use incr(diff_count + init_count) or set_count instead.
  if (TAIR_RETURN_SUCCESS != (ret = client_helper->incr(area, key, diff_count, &ret_count, init_count, 0)))
  {
    cout << "incr failed. key: " << key.get_data() << ", ret: " << ret << endl; 
  }
  else
  {
    cout << "incr succ. key: " << key.get_data() << ", ret count: " << ret_count << endl;
  }

  if (TAIR_RETURN_SUCCESS != (ret = client_helper->decr(area, key, diff_count - 1, &ret_count, 0, 0)))
  {
    cout << "decr failed. key: " << key.get_data() << ", ret: " << ret << endl;
  }
  else
  {
    cout << "decr succ. key: " << key.get_data() << ", ret count: " << ret_count << endl;
  }

  if (TAIR_RETURN_SUCCESS != (ret = client_helper->get(area, key, ret_value)))
  {
    cout << "get failed. key: " << key.get_data() << ", ret: " << ret << endl;
  }
  else
  {
    cout << "get succ. key: " << key.get_data() << ", value: " << ret_value->get_data() << ", ret: " << ret << endl;
    int cur_count = atoi(ret_value->get_data());
    delete ret_value;
    ret_value = NULL;

    if (TAIR_RETURN_SUCCESS != (ret = client_helper->set_count(area, key, cur_count + 10)))
    {
      cout << "set_count failed. key: " << key.get_data() << ", ret: " << ret << endl; 
    }
    else
    {
      cout << "set_count succ. key: " << key.get_data() << endl; 
    }
  }

  ret = client_helper->remove(area, key);
  cout << "remove key: " << key.get_data() << ", ret" << ret<< endl; 
}

// !! only for ldb
void range_api_test(tair_client_api *client_helper)
{
  data_entry pkey("pkey");
  tair_dataentry_vector mkeys;
  int ret = TAIR_RETURN_SUCCESS;
  
  for (int i = 0; i < loop_times; i++)
  {
    char buf[16];
    sprintf(buf, "%s_%d", "key", i);
    data_entry* tmp_skey = new data_entry(buf); // it will alloc data by default
    sprintf(buf, "%s_%d", "value", i);
    data_entry* tmp_value = new data_entry(buf); // it will alloc data by default
    if (TAIR_RETURN_SUCCESS != (ret = client_helper->prefix_put(area, pkey, *tmp_skey, *tmp_value, 0, 0)))
    {
      cout << "prefix put failed. key: " << tmp_skey->get_data() << ", value: " << tmp_value->get_data() << ", ret: " << ret << endl;
    }
    else
    {
      cout << "prefix put succ. key: " << tmp_skey->get_data() << ", value: " << tmp_value->get_data() << ", ret: " << ret << endl;
    }
    delete tmp_skey;
    tmp_skey = NULL;
    delete tmp_value;
    tmp_value = NULL;
  }

  vector<data_entry *> values;
  vector<data_entry *>::iterator iter;
  data_entry start_key("key_1");
  data_entry end_key("key_2");
  ret = client_helper->get_range(area, pkey, start_key, end_key, 0, 10, values, CMD_RANGE_VALUE_ONLY);
  if (ret != TAIR_RETURN_SUCCESS)
  {
    cout << "get range faild. pkey: " << pkey.get_data() << ", ret: " << ret << endl;
  }
  else
  {
    iter = values.begin();
    for (; iter != values.end(); iter++)
    {
      cout << "value: " << (*iter)->get_data() << endl;
      delete (*iter);
    }
    values.clear();
  }
  // get all keys and values
  ret = TAIR_HAS_MORE_DATA;

  while (TAIR_HAS_MORE_DATA == ret)
  {
    vector<data_entry *> tmp_values;
    ret = client_helper->get_range(area, pkey, "", "", 0, MAGIC_NUM, tmp_values, CMD_RANGE_ALL);
    if ((ret != TAIR_RETURN_SUCCESS) && (ret != TAIR_HAS_MORE_DATA))
    {
      cout << "get range faild. pkey: " << pkey.get_data() << ", ret: " << ret << endl;
      break;
    }
    else
    {
      values.insert(values.end(), tmp_values.begin(), tmp_values.end());
    }
  }
  // CMD_RANGE_ALL will return keys and values, its order will be key1/value1/key2/value2....
  iter = values.begin();
  int index = 0;
  for (; iter != values.end(); iter++)
  {
    if (index % 2 == 0)
    {
      cout << "key: " << (*iter)->get_data();
      ret = client_helper->prefix_remove(area, pkey, *(*iter));
    }
    else
    {
      cout << ", value: " << (*iter)->get_data() << endl;
    }
    index++;
    delete (*iter);
    *iter = NULL;
  }
  values.clear();
}
