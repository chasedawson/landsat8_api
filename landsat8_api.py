# import libraries
import json
import requests
import sys
import time
import threading
import re

class Landsat8_API_Accessor:
    def __init__(self, username, password):
        # API base URL
        self.SERVICE_URL = "https://m2m.cr.usgs.gov/api/api/json/stable/"
        self.apiKey = self.login(username, password)
        self.threads = []
        self.MAX_THREADS = 5
        self.sema = threading.Semaphore(value=self.MAX_THREADS)

    def __add_list(self, list_id, entity_ids, dataset = 'landsat_ot_c2_l2', logging = False):
        print("__add_list called")
        payload = {
            'listId': list_id,
            'datasetName': dataset,
            'entityIds': entity_ids,
        }
        count = self.sendRequest(self.SERVICE_URL + "scene-list-add", payload, self.apiKey)
        if logging:
            print('Added', count, 'scenes to list', list_id)

    def __remove_list(self, list_id):
        print("__remove_list called")
        payload = {
            'listId': list_id,
        }
        self.sendRequest(self.SERVICE_URL + "scene-list-remove", payload, self.apiKey)

    def __select_products(self, products, download_type, file_types = None):
        print("__select_products called")
        downloads = []
        if download_type == "bundle":
            downloads = self.__get_bulk(products, downloads)
        elif download_type == "band":
            downloads = self.__get_secondaryDownloads(products, downloads, file_types=file_types)
        else:
            downloads = self.__get_bulk(products, downloads)
            downloads = self.__get_secondaryDownloads(products, downloads)
        return downloads

    def __get_product_download_options(self, list_id, dataset = 'landsat_ot_c2_l2', logging = False):
        print("__get_product_download_options called")
        payload = {
            "listId": list_id,
            "datasetName": dataset,
        }
        products = self.sendRequest(self.SERVICE_URL + "download-options", payload, self.apiKey)
        return products

    def __get_bulk(self, products, downloads):
        print("__get_bulk called")
        for product in products:
            if product['bulkAvailable']:
                downloads.append({"entityId": product["entityId"], "productId": product["id"]})
        return downloads

    def __get_secondaryDownloads(self, products, downloads, file_types = None):
        print("__get_secondaryDownloads called")
        for product in products:
            if product["secondaryDownloads"] is not None and len(product["secondaryDownloads"]) > 0:
                for secondaryDownload in product["secondaryDownloads"]:
                    if secondaryDownload["bulkAvailable"]:
                        if file_types is None:
                            downloads.append({"entityId": secondaryDownload["entityId"], "productId": secondaryDownload["id"], "scene_entityId": product["entityId"]})
                        else:
                            for file_type in file_types:
                                if secondaryDownload["entityId"][-len(file_type):] == file_type:
                                    downloads.append({"entityId": secondaryDownload["entityId"], "productId": secondaryDownload["id"], "scene_entityId": product["entityId"]})
                                    break # stop iterating through filetypes for this secondaryDownload, filetype has been found
        return downloads

    def __request_downloads(self, downloads, label, logging = False):
        print("__request_downloads called")
        payload = {
            'downloads': downloads,
            "label": label,
            'returnAvailable': True
        }

        results = self.sendRequest(self.SERVICE_URL + "download-request", payload, self.apiKey)
        return results

    def __retrieve_downloads(self, label):
        print("__retrieve_downloads called")
        payload = {
            "label": label,
        }
        results = self.sendRequest(self.SERVICE_URL + "download-retrieve", payload, self.apiKey)
        return results

    def __runDownload(self, threads, url, downloaded):
        print("__runDownload called")
        thread = threading.Thread(target=self.download_file, args=(url, downloaded,))
        threads.append(thread)
        thread.start()

    def __download(self, downloads, label = "test", max_wait_time = 300, logging = True):
        """
        Return entity_ids of downloaded files.

        """
        print("__download called")
        downloaded = []
        scene_entityId_map = {x['entityId']: x['scene_entityId'] for x in downloads}

        dq_results = self.__request_downloads([{'entityId': x['entityId'], 'productId': x['productId']} for x in downloads], label)
        for result in dq_results['availableDownloads']:
            # to_download = {'entityId': result['entityId'], 'url': result['url']}
            self.__runDownload(self.threads, result['url'], downloaded)

        cur_wait_time = 0
        preparingDownloadCount = len(dq_results['preparingDownloads'])
        preparingDownloadIds = []
        if preparingDownloadCount > 0:
            if logging:
                print("Preparing {count} downloads.".format(count = preparingDownloadCount))
            for result in dq_results['preparingDownloads']:
                preparingDownloadIds.append(result['downloadId'])

            dr_results = self.__retrieve_downloads(label)
            if dr_results != False:
                for result in dr_results['available']:
                    if result['downloadId'] in preparingDownloadIds:
                        preparingDownloadIds.remove(result['downloadId'])
                        # to_download = {'entityId': result['entityId'], 'url': result['url']}
                        self.__runDownload(self.threads, result['url'], downloaded)

                for result in dr_results['requested']:
                    if result['downloadId'] in preparingDownloadIds:
                        preparingDownloadIds.remove(result['downloadId'])
                        # to_download = {'entityId': result['entityId'], 'url': result['url']}
                        self.__runDownload(self.threads, result['url'], downloaded)

            while len(preparingDownloadIds) > 0:
                if logging:
                    print("{num_waiting} downloads are not available yet. Waiting for 30s to retrieve again.".format(num_waiting = len(preparingDownloadIds)))
                
                if cur_wait_time >= max_wait_time:
                    print("{num_waiting} downloads are still unavailable. Exiting download process.".format(num_waiting = len(preparingDownloadIds)))
                    break
                else:
                    time.sleep(30)
                    cur_wait_time += 30
                    dr_results = self.__retrieve_downloads(label)
                    if dr_results != False:
                        for result in dr_results['available']:
                            if result['downloadId'] in preparingDownloadIds:
                                preparingDownloadIds.remove(result['downloadId'])
                                # to_download = {'entityId': result['entityId'], 'url': result['url']}
                                self.__runDownload(self.threads, result['url'], downloaded)

        # wait for all threads to finish running
        [t.join() for t in self.threads]

        # reformat downloaded
        downloaded = [{"entityId": x, "scene_entityId": scene_entityId_map[x]} for x in downloaded]

        return downloaded
    
    def __getFilename_fromCd(self, cd):
        """
        Uses content-disposition to infer filename and filetype.
        
        Parameters
        ----------
        cd : str, required
            The Content-Disposition response header from HTTP request 
            to download a file.
            
        Output
        ------
        Inferred filename and type of provided file : str  
        """
        print("__getFilename_fromCd called")
        if not cd:
            return None
        fname = re.findall('filename=(.+)', cd)
        if len(fname) == 0:
            return None
        
        return re.sub('\"', '', fname[0]) # remove extra quotes
    
    def download_file(self, url, downloaded):
        """
        Saves file to local system.
        
        Parameters
        ----------
            url: str, required
                Link to file to be downloaded.
                
        Output
        ------
        Path to downloaded file : str
        """
        print("download_file called")
        self.sema.acquire()
        try:
            res = requests.get(url, stream=True)
            filename = self.__getFilename_fromCd(res.headers.get('content-disposition'))
            print("Downloading {filename}...".format(filename = filename))
            open(filename, 'wb').write(res.content)
            print('Downloaded {filename}.'.format(filename = filename))
            entity_id = "L2ST_" + filename.split('.')[0] + "_TIF"
            downloaded.append(entity_id)
            self.sema.release()
        except Exception as e:
            print("Failed to download from {url}. Will try to re-download.".format(url = url))
            self.sema.release()
            self.runDownload(self.threads, url, downloaded)
    
    def login(self, username, password):
        """
        Authenticates user given username and password and returns API key.
        
        Parameters
        ----------
        username : str, required
            USGS account username.
            
        password : str, required
            USGS account password. 
            
        Notes 
        -----
        Go to https://ers.cr.usgs.gov/profile/access to request access 
        to the API and/or make an account.
        
        """
        print("login called")
        # login information
        payload = {'username': username, 'password': password}

        # get apiKey 
        apiKey = self.sendRequest(self.SERVICE_URL + "login", payload)
        if apiKey == None:
            print("Login Failed")
        else:
            print("Login Successful")
        
        return apiKey

    def logout(self):
        """
        Invalidates API key. 
        
        Parameters
        ----------
        apiKey : str, required
            Valid API key. Obtain using the login() method defined above.
            
        Notes
        -----
        Make sure to call when you've finished working to ensure that your 
        API key can't be used by an unauthorized user.
        
        """
        print("logout called")
        if self.sendRequest(self.SERVICE_URL + "logout", None, self.apiKey) == None:
            print("Logged Out\n\n")
        else:
            print("Logout Failed\n\n")

    def sendRequest(self, url, data, apiKey = None):
        """
        Sends HTTPS request to specified API endpoint. Main method for interacting
        with the API.
        
        Parameters
        ----------
        url : str, required
            API endpoint you wish you access. Typical format is SERVICE_URL + endpoint, 
            where endpoint might be something like "login" or "data-search." See https://m2m.cr.usgs.gov/api/docs/reference/
            for all available endpoints.
            
        data : dict, required
            Request payload. Data required changes based on API endpoint. See 
            https://m2m.cr.usgs.gov/api/docs/reference/ for input parameters, sample requests,
            sample and responses for available endpoints.
            
        apiKey : str, optional (default is None)
            Valid API key. Must be speficied for most requests. "login" endpoint doesn't 
            require an API key since you use that endpoint to retrieve a valid API key.
        
        """
        print("sendRequest called")
        json_data = json.dumps(data)
        
        if apiKey == None:
            response = requests.post(url, json_data)
        else:
            headers = {'X-Auth-Token': apiKey}
            response = requests.post(url, json_data, headers = headers)
            
        try:
            httpStatusCode = response.status_code
            
            if response == None:
                print("No output from service!")
                sys.exit()
                
            output = json.loads(response.text)
            if output['errorCode'] != None:
                print(output['errorCode'], "- ", output['errorMessage'])
                sys.exit()
                
            if httpStatusCode == 404:
                print("404 Not Found")
                sys.exit()
                
            elif httpStatusCode == 401:
                print("401 Unauthorized")
                sys.exit()
                
            elif httpStatusCode == 400:
                print("Error Code", httpStatusCode)
                sys.exit()
                
        except Exception as e:
            response.close()
            print(e)
            sys.exit()
        
        response.close()
        return output['data']

    def download_scenes_from_entity_ids(self, entity_ids, list_id = "test", logging = False):
        print("download_scenes_from_entity_ids called")
        self.__add_list(list_id, entity_ids)
        products = self.__get_product_download_options(list_id)
        downloads = self.__select_products(products, download_type="band", file_types=["ST_B10_TIF"])
        self.__remove_list(list_id)
        downloaded = self.__download(downloads, label = "{list_id}_{t}".format(list_id = list_id, t = str(time.time()).split('.')[0]))
        return downloaded
