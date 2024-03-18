import requests
import json
from concurrent.futures import ThreadPoolExecutor
import os
from collections import namedtuple

ItemInfo = namedtuple("ItemInfo", ["id", "name", "image_url", "nutrition", "is_dummy", "category"])


class BKClient:
    
    def __init__(self):
        
        self.menu_template = """https://use1-prod-bk-gateway.rbictg.com/graphql?operationName=storeMenu&variables=%%7B%%22channel%%22%%3A%%22whitelabel%%22%%2C%%22region%%22%%3A%%22US%%22%%2C%%22storeId%%22%%3A%%22%s%%22%%2C%%22serviceMode%%22%%3A%%22pickup%%22%%7D&extensions=%%7B%%22persistedQuery%%22%%3A%%7B%%22version%%22%%3A1%%2C%%22sha256Hash%%22%%3A%%2248a3fa9cd76ee8e29027ab0d4d13bf5bfb1eca856f312735fa572a2c3acec90b%%22%%7D%%7D"""
        self.nearby_store_template = """https://use1-prod-bk-gateway.rbictg.com/graphql?operationName=GetNearbyRestaurants&variables=%%7B%%22input%%22%%3A%%7B%%22pagination%%22%%3A%%7B%%22first%%22%%3A100%%7D%%2C%%22radiusStrictMode%%22%%3Afalse%%2C%%22coordinates%%22%%3A%%7B%%22searchRadius%%22%%3A10000000%%2C%%22userLat%%22%%3A%s%%2C%%22userLng%%22%%3A%s%%7D%%7D%%7D&extensions=%%7B%%22persistedQuery%%22%%3A%%7B%%22version%%22%%3A1%%2C%%22sha256Hash%%22%%3A%%221d288d2ae206ab197a3a9aff0d7cf8997b2842cbe21dea7fac94cc8a92acdb43%%22%%7D%%7D"""
        self.store_info_template = """https://czqk28jt.apicdn.sanity.io/v1/graphql/prod_bk_us/default?operationName=GetRestaurants&variables=%%7B%%22filter%%22%%3A%%7B%%22_id%%22%%3A%%22%s%%22%%7D%%2C%%22limit%%22%%3A1%%7D&query=query+GetRestaurants%%28%%24filter%%3ARestaurantFilter%%24limit%%3AInt%%29%%7BallRestaurants%%28where%%3A%%24filter+limit%%3A%%24limit%%29%%7B...RestaurantFragment+__typename%%7D%%7Dfragment+RestaurantFragment+on+Restaurant%%7B_id+environment+chaseMerchantId+deliveryHours%%7B...HoursFragment+__typename%%7DdiningRoomHours%%7B...HoursFragment+__typename%%7DcurbsideHours%%7B...HoursFragment+__typename%%7DdriveThruHours%%7B...HoursFragment+__typename%%7DdrinkStationType+driveThruLaneType+email+fastestServiceMode+franchiseGroupId+franchiseGroupName+frontCounterClosed+hasBreakfast+hasBurgersForBreakfast+hasCurbside+hasDineIn+hasCatering+hasDelivery+hasDriveThru+hasMobileOrdering+hasParking+hasPlayground+hasTakeOut+hasWifi+hasLoyalty+isDarkKitchen+isHalal+latitude+longitude+mobileOrderingStatus+name+number+parkingType+phoneNumber+playgroundType+pos%%7B_type+vendor+__typename%%7DphysicalAddress%%7B_type+address1+address2+city+country+postalCode+stateProvince+__typename%%7DposRestaurantId+restaurantPosData%%7B_id+__typename%%7Dstatus+restaurantImage%%7Basset%%7B...ImageAssetFragment+__typename%%7D__typename%%7Damenities%%7Bname%%7Blocale%%3Aen+__typename%%7Dicon%%7Basset%%7B...ImageAssetFragment+__typename%%7D__typename%%7D__typename%%7Dtimezone+vatNumber+__typename%%7Dfragment+HoursFragment+on+HoursOfOperation%%7B_type+friClose+friOpen+monClose+monOpen+satClose+satOpen+sunClose+sunOpen+thrClose+thrOpen+tueClose+tueOpen+wedClose+wedOpen+__typename%%7Dfragment+ImageAssetFragment+on+SanityImageAsset%%7B_id+label+title+url+source%%7Bid+url+__typename%%7Dmetadata%%7BblurHash+__typename%%7D__typename%%7D"""

        self.item_info_template = """https://czqk28jt.apicdn.sanity.io/v1/graphql/prod_bk_us/default?operationName=GetPicker&variables=%7B%22id%22%3A%22{item_id}%22%7D&query={query}"""

        # load the item info query from a file
        with open(f"Queries{os.sep}ItemInfo.gql", "r") as f:
            self.item_info_query = f.read()


    def any_not_in(self, d, key_list):
        """
        Checks if the hierarchy of keys in key_list exists in the dictionary d.

        Args:
            d (dict): The dictionary to check.
            key_list (list): A list of keys that form the hierarchy to check.

        Returns:
            bool: True if the hierarchy of keys exists in the dictionary, False otherwise.
        """

        for key in key_list:
            if not d or key not in d:
                return True
            d = d[key]

        return False
    

    def key_sequence_or_none(self, d, key_list):
        """
        If the list of keys exists in the dictionary, return the value. Otherwise, return None.

        Args:
            d (dict): The dictionary to check.
            key_list (list): A list of keys that form the hierarchy to check.

        Returns:
            dict: The value at the end of the key hierarchy, or None if the hierarchy does not exist.
        """

        result = None

        cur = 0
        cur_key = key_list[cur]

        while cur_key in d or (type(d) == list and type(cur_key) == int and len(d) > cur_key):
            d = d[cur_key]
            cur += 1
            if cur == len(key_list):
                result = d
                break
            cur_key = key_list[cur]

        return result


    def get_menu(self, store_id, session=None):
        """
        Fetches the menu for a Burger King store.

        Args:
            store_id (str): The store ID for which the menu needs to be fetched.
            session (requests.Session, optional): A requests session object to use for the request. Defaults to None.

        Returns:
            dict: The menu for the specified store, or None if the menu cannot be fetched.
        """

        url = self.menu_template % store_id

        if session:
            resp = session.get(url)
        else:
            resp = requests.get(url)

        if resp.status_code == 200:
            j = resp.json()

        if 'data' in j and 'storeMenu' in j['data']:
            return j['data']['storeMenu']
        

    def get_many_menus(self, menus, threads=1):
        """
        Fetches menus for multiple Burger King stores concurrently.

        Args:
            menus (list): A list of store IDs for which the menus need to be fetched.
            threads (int, optional): The number of threads to use for concurrent requests. Defaults to 1.

        Returns:
            dict: A dictionary mapping store IDs to their corresponding menus. If a menu cannot be fetched for a store (due to an invalid store ID or a failed request), the store ID will not be included in the returned dictionary.
        """

        results = {}
        
        with ThreadPoolExecutor(max_workers=threads) as executor:
            session = requests.Session()
            futures = {store_id: executor.submit(self.get_menu, store_id, session) for store_id in menus}

            for store_id, future in futures.items():
                result = future.result()
                if result:
                    results[store_id] = result

        return results
    

    def get_nearby_stores(self, lat, lon, session=None, ids_only=False):
        """
        Fetches nearby Burger King stores based on latitude and longitude.

        Args:
            lat (float): The latitude of the location.
            lon (float): The longitude of the location.
            session (requests.Session, optional): A requests session object to use for the request. Defaults to None.
            ids_only (bool, optional): If True, only store IDs will be returned. If False, store information dictionaries will be returned. Defaults to False.

        Returns:
            list: A list of store IDs if ids_only is True, or a list of store information dictionaries if ids_only is False. Returns an empty list if no stores are found.
        """

        url = self.nearby_store_template % (lat, lon)

        if session:
            resp = session.get(url)
        else:
            resp = requests.get(url)

        if resp.status_code == 200:
            j = resp.json()

        if self.any_not_in(j, ['data', 'restaurantsV2', 'nearby', 'nodes']):
            return []
        
        if ids_only:
            return [store['storeId'] for store in j['data']['restaurantsV2']['nearby']['nodes']]
        else:
            return j['data']['restaurantsV2']['nearby']['nodes']
        

    def get_many_nearby_stores(self, locations, threads=1):
        """
        Fetches nearby Burger King stores for multiple locations concurrently.

        Args:
            locations (list): A list of (latitude, longitude) tuples for which nearby stores need to be fetched.
            threads (int, optional): The number of threads to use for concurrent requests. Defaults to 1.

        Returns:
            dict: A dictionary mapping store IDs to their corresponding store information. If information cannot be fetched for a store (due to an invalid store ID or a failed request), the store ID will not be included in the returned dictionary.
        """

        results = {}
        
        with ThreadPoolExecutor(max_workers=threads) as executor:
            session = requests.Session()
            futures = [executor.submit(self.get_nearby_stores, lat, lon, session) for lat, lon in locations]

            for future in futures:
                result = future.result()
                if result:
                    results.update({store['storeId']: store for store in result})

        return results
    
    
    def get_store_info(self, restaurant_id, session=None):
        """
        Fetches information about a Burger King store.

        Args:
            restaurant_id (str): The restaurant id (usually restaurant_somenumber) for which the information needs to be fetched.
            session (requests.Session, optional): A requests session object to use for the request. Defaults to None.

        Returns:
            dict: Information about the specified store, or None if the information cannot be fetched.
        """

        url = self.store_info_template % restaurant_id

        if session:
            resp = session.get(url)
        else:
            resp = requests.get(url)

        if resp.status_code == 200:
            j = resp.json()

        if "data" in j and "allRestaurants" in j["data"]:
            return j["data"]["allRestaurants"][0]
        

    def get_many_store_info(self, restaurant_ids, threads=1):
        """
        Fetches information about multiple Burger King stores concurrently.

        Args:
            restaurant_ids (list): A list of restaurant IDs for which the information needs to be fetched.
            threads (int, optional): The number of threads to use for concurrent requests. Defaults to 1.

        Returns:
            dict: A dictionary mapping restaurant IDs to their corresponding store information. If information cannot be fetched for a store (due to an invalid restaurant ID or a failed request), the restaurant ID will not be included in the returned dictionary.
        """

        results = {}
        
        with ThreadPoolExecutor(max_workers=threads) as executor:
            session = requests.Session()
            futures = {restaurant_id: executor.submit(self.get_store_info, restaurant_id, session) for restaurant_id in restaurant_ids}

            for restaurant_id, future in futures.items():
                result = future.result()
                if result:
                    results[restaurant_id] = result

        return results
    

    def get_item_info(self, item_id, session=None):
        """
        Fetches information about a Burger King menu item.

        Args:
            item_id (str): The item ID for which the information needs to be fetched.
            session (requests.Session, optional): A requests session object to use for the request. Defaults to None.

        Returns:
            ItemInfo: An ItemInfo named tuple containing information about the specified item, or None if the information cannot be fetched.
        """

        url = self.item_info_template.format(item_id=item_id, query=self.item_info_query)

        if session:
            resp = session.get(url)
        else:
            resp = requests.get(url)

        if resp.status_code == 200:
            j = resp.json()

        if "data" not in j:
            print("Data not in j")
            return None

        data = j["data"]

        if type(data) != dict or 'Picker' not in data or data['Picker'] is None:
            return None
        

        # ItemInfo(item_id, "Name", "Image", "Nutrition", False, "Category")
        item_id = item_id
        name = self.key_sequence_or_none(data, ['Picker', 'name', 'locale'])
        image_url = self.key_sequence_or_none(data, ['Item', 'image', 'asset', 'url'])
        if image_url is not None and image_url.startswith("image-"):
            image_url = f"https://cdn.sanity.io/images/czqk28jt/prod_bk_us/{image_url}"
        nutrition = self.key_sequence_or_none(data, ['Item', 'nutrition'])
        is_dummy = self.key_sequence_or_none(data, ['Item', 'isDummyItem']) != None
        hierarchy = self.key_sequence_or_none(data, ['Item', 'productHierarchy', 'L2'])


        if image_url is None:
            image_url = self.key_sequence_or_none(data, ['Picker', 'options', 0, 'option', 'image', 'asset', 'url'])

        if nutrition is None:
            nutrition = self.key_sequence_or_none(data, ['Picker', 'options', 0, 'option', 'nutrition'])

        if hierarchy is None:
            hierarchy = self.key_sequence_or_none(data, ['Picker', 'options', 0, 'option', 'productHierarchy', 'L2'])


        return ItemInfo(item_id, name, image_url, nutrition, is_dummy, hierarchy)

            
            
    

    def get_many_item_info(self, item_ids, threads=1):
        """
        Fetches information about multiple Burger King menu items concurrently.

        Args:
            item_ids (list): A list of item IDs for which the information needs to be fetched.
            threads (int, optional): The number of threads to use for concurrent requests. Defaults to 1.

        Returns:
            dict: A dictionary mapping item IDs to their corresponding item information. If information cannot be fetched for an item (due to an invalid item ID or a failed request), the item ID will not be included in the returned dictionary.
        """

        results = {}
        
        with ThreadPoolExecutor(max_workers=threads) as executor:
            session = requests.Session()
            futures = {item_id: executor.submit(self.get_item_info, item_id, session) for item_id in item_ids}

            for item_id, future in futures.items():
                result = future.result()
                if result:
                    results[item_id] = result

        return results
    

    def search_lat_lon(self, lat_start, lat_end, lon_start, lon_end, increment=0.5, ids_only=True):
        """
        Given starting and ending coordinates search the area for Burger King locations and return a list of store IDs if ids_only is True, otherwise a list of dictionary objects representing stores.

        Args:
            lat_start (float): The starting latitude of the search area.
            lat_end (float): The ending latitude of the search area.
            lon_start (float): The starting longitude of the search area.
            lon_end (float): The ending longitude of the search area.

        Returns:
            list: A list of store IDs if ids_only is True, or a list of store information dictionaries if ids_only is False. Returns an empty list if no stores are found.
        """

        intersections = []

        cur_lat = lat_start
        cur_lon = lon_start

        while cur_lat > lat_end:
            while cur_lon < lon_end:
                intersections.append((cur_lat, cur_lon))
                cur_lon += increment
            cur_lat -= increment
            cur_lon = lon_start

        bks = self.get_many_nearby_stores(intersections, threads=10)

        return bks