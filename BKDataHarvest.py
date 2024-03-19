from BKClient import BKClient
import time
import csv
import os
import argparse
import asyncpg
import asyncio
from datetime import datetime

bkc = BKClient()


def search_usa():

    contiguous_states = {
        "lat_start": 49.384358,
        "lat_end": 24.396308,
        "lon_start": -124.848974,
        "lon_end": -66.885444
    }

    hawaii = {
        "lat_start": 22.533,
        "lat_end": 18.709,
        "lon_start": -160.950,
        "lon_end": -154.490
    }

    alaska = {
        "lat_start": 71.371,
        "lat_end": 55.304,
        "lon_start": -169.233,
        "lon_end": -140.669
    }

    contiguous_states = bkc.search_lat_lon(**contiguous_states)
    hawaii = bkc.search_lat_lon(**hawaii)
    alaska = bkc.search_lat_lon(**alaska)

    return {**contiguous_states, **hawaii, **alaska}


def simple_menu_item(store_id, item):
    """
    If the item has a valid id, isAvailable, price, and calories, return a tuple of those values.  Otherwise, return None.

    Args:
        item (dict): A dictionary representing a menu item.

    Returns:
        tuple: A tuple of the item's store_id, id, isAvailable, price_min, price_max, price_default, and avg_calories.  If any of these values are missing, return None.
    """
    
    item_id = item.get('id')
    is_available = item.get('isAvailable')
    price = item.get('price', None)

    if price == None:
        return None
    
    price_min = price.get('min')
    price_max = price.get('max')
    price_default = price.get('default')

    if price_min * price_max * price_default == 0:
        return None

    calories = item.get('calories', None)

    if calories == None or calories == 0:
        return None
    
    avg_calories = (calories.get('min') + calories.get('max')) / 2

    result = (store_id, item_id, is_available, price_min, price_max, price_default, avg_calories)

    return result


def simple_menu(store_id, menu):
    """
    Return a list of simple_menu_item tuples for each item in the menu.

    Args:
        menu (list): A list of dictionaries representing menu items.

    Returns:
        list: A list of simple_menu_item tuples.
    """

    result = [simple_menu_item(store_id, item) for item in menu]

    return [item for item in result if item is not None]


def simple_restaurant(restaurant):
    """
    Extracts selected information from a restaurant object.

    Args:
        restaurant (dict): A dictionary representing a restaurant.

    Returns:
        tuple: A tuple of the restaurant's id, storeId, city, state, postalCode, latitude, longitude,
               status, hasBreakfast, hasDelivery, hasDineIn, hasDriveThru, hasMobileOrdering, hasTakeOut,
               posVendor, and total_weekly_hours.
               If any of these values are missing, return None for that value.
    """
    restaurant_id = restaurant.get('id')
    store_id = restaurant.get('storeId')
    address = restaurant.get('physicalAddress', {})
    city = address.get('city')
    state = address.get('stateProvince')
    postal_code = address.get('postalCode')
    latitude = restaurant.get('latitude')
    longitude = restaurant.get('longitude')
    status = restaurant.get('status')
    has_breakfast = restaurant.get('hasBreakfast')
    has_delivery = restaurant.get('hasDelivery')
    has_dine_in = restaurant.get('hasDineIn')
    has_drive_thru = restaurant.get('hasDriveThru')
    has_mobile_ordering = restaurant.get('hasMobileOrdering')
    has_take_out = restaurant.get('hasTakeOut')
    pos_vendor = restaurant.get('posVendor')

    # Calculate total weekly hours
    total_weekly_hours = 0
    for day in ['mon', 'tue', 'wed', 'thr', 'fri', 'sat', 'sun']:
        open_time = restaurant.get('diningRoomHours', {}).get(f'{day}Open')
        close_time = restaurant.get('diningRoomHours', {}).get(f'{day}Close')
        if open_time and close_time:
            open_hour, open_minute, _ = map(int, open_time.split(':'))
            close_hour, close_minute, _ = map(int, close_time.split(':'))
            total_weekly_hours += (close_hour * 60 + close_minute - open_hour * 60 - open_minute) / 60

    result = (restaurant_id, store_id, city, state, postal_code, latitude, longitude, status,
              has_breakfast, has_delivery, has_dine_in, has_drive_thru, has_mobile_ordering, has_take_out,
              pos_vendor, total_weekly_hours)

    return result


def simple_restaurants(restaurants):
    """
    Return a list of simple_restaurant tuples for each restaurant in the list.

    Args:
        restaurants (list): A list of dictionaries representing restaurants.

    Returns:
        list: A list of simple_restaurant tuples.
    """

    result = [simple_restaurant(restaurant) for restaurant in restaurants]

    return [restaurant for restaurant in result if restaurant is not None]


def write_menu_items_to_csv(store_ids):
    """
    Use the BKClient to get the menu items for the given store ids and write the menu items to a CSV file.

    Args:
        store_ids (list): A list of store ids.
        file_name (str): The name of the CSV file to write the menu items to.

    Returns:
        item_ids (set): A set of item ids.
    """

    save_prefix = time.strftime("%Y-%m-%d-")

    all_item_ids = set()

    created_date = time.strftime("%Y-%m-%d")

    with open(f'Temp{os.sep}{save_prefix}bk_data.csv', 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['store_id', 'item_id', 'isAvailable', 'price_min', 'price_max', 'price_default', 'avg_calories', 'created_date'])

        batch_increment = 100
        cur, end = 0, batch_increment
        while cur < len(store_ids):
            print(f"Starting {cur} of {len(store_ids)}")
            cur_ids = store_ids[cur:end]
            all_menus = bkc.get_many_menus(cur_ids, threads=10)

            # Save the data to a CSV file
            rows = []
            for store_id, menu in all_menus.items():
                rows += simple_menu(store_id, menu)
            
            # append created_date to each row
            for i in range(len(rows)):
                rows[i] = rows[i] + (created_date,)
            writer.writerows(rows)

            cur += batch_increment
            end += batch_increment

            # keep track of item ids
            for row in rows:
                all_item_ids.add(row[1])

            print(f"Finished {min(cur, len(store_ids))} of {len(store_ids)}")

    return all_item_ids


def whole_harvest(upload=False):
    stores = search_usa()
    store_ids = list(stores.keys())
    # prefix for current date YYYY-MM-DD-
    save_prefix = time.strftime("%Y-%m-%d-")

    with open(f'Temp{os.sep}{save_prefix}bk_restaurants.csv', 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['restaurant_id', 'store_id', 'city', 'state', 'postal_code', 'latitude', 'longitude', 'status', 'has_breakfast', 'has_delivery', 'has_dine_in', 'has_drive_thru', 'has_mobile_ordering', 'has_take_out', 'pos_vendor', 'total_weekly_hours'])

        rows = simple_restaurants(stores.values())

        writer.writerows(rows)
    stores = None

    all_item_ids = write_menu_items_to_csv(store_ids)

    if upload:
        asyncio.run(upload_to_db(restaurants=f'Temp{os.sep}{save_prefix}bk_restaurants.csv', menu_items=f'Temp{os.sep}{save_prefix}bk_data.csv'))

    print("Finished all stores")

    all_item_infos = bkc.get_many_item_info(list(all_item_ids), threads=10)

    with open(f'Temp{os.sep}{save_prefix}bk_items.csv', 'w', newline='') as file:
        # ItemInfo(id='picker_5520', name='Whopper', image_url='https://cdn.sanity.io/images/czqk28jt/prod_bk_us/e8dfc0b5c84670d64195a6602ef7f99eb70fe764-1333x1333.png', nutrition={'calories': 485.215, 'fat': 21.705, 'saturatedFat': 9, 'transFat': 0, 'cholesterol': 70, 'sodium': 583.475, 'carbohydrates': 46.92, 'fiber': 1.975, 'sugar': 8.63, 'proteins': 30.5}, is_dummy=False)
        writer = csv.writer(file)

        writer.writerow(['item_id', 'name', 'image_url', 'calories', 'fat', 'saturatedFat', 'transFat', 'cholesterol', 'sodium', 'carbohydrates', 'fiber', 'sugar', 'proteins', 'is_dummy', 'category'])

        rows = []
        for item_id, item_info in all_item_infos.items():
            row = (item_id, item_info.name, item_info.image_url, item_info.nutrition['calories'] if item_info.nutrition else "", item_info.nutrition['fat'] if item_info.nutrition else "", item_info.nutrition['saturatedFat'] if item_info.nutrition else "", item_info.nutrition['transFat'] if item_info.nutrition else "", item_info.nutrition['cholesterol'] if item_info.nutrition else "", item_info.nutrition['sodium'] if item_info.nutrition else "", item_info.nutrition['carbohydrates'] if item_info.nutrition else "", item_info.nutrition['fiber'] if item_info.nutrition else "", item_info.nutrition['sugar'] if item_info.nutrition else "", item_info.nutrition['proteins'] if item_info.nutrition else "", item_info.is_dummy, item_info.category)
            rows.append(row)

        writer.writerows(rows)

    print("Finished all items")

    if upload:
        asyncio.run(upload_to_db(item_info=f'Temp{os.sep}{save_prefix}bk_items.csv'))



def simple_restaurant(restaurant):
    """
    Extracts selected information from a restaurant object.

    Args:
        restaurant (dict): A dictionary representing a restaurant.

    Returns:
        tuple: A tuple of the restaurant's id, storeId, city, state, postalCode, latitude, longitude,
               status, hasBreakfast, hasDelivery, hasDineIn, hasDriveThru, hasMobileOrdering, hasTakeOut,
               posVendor, and total_weekly_hours.
               If any of these values are missing, return None for that value.
    """
    restaurant_id = restaurant.get('id')
    store_id = restaurant.get('storeId')
    address = restaurant.get('physicalAddress', {})
    city = address.get('city')
    state = address.get('stateProvince')
    postal_code = address.get('postalCode')
    latitude = restaurant.get('latitude')
    longitude = restaurant.get('longitude')
    status = restaurant.get('status')
    has_breakfast = restaurant.get('hasBreakfast')
    has_delivery = restaurant.get('hasDelivery')
    has_dine_in = restaurant.get('hasDineIn')
    has_drive_thru = restaurant.get('hasDriveThru')
    has_mobile_ordering = restaurant.get('hasMobileOrdering')
    has_take_out = restaurant.get('hasTakeOut')
    pos_vendor = restaurant.get('posVendor')

    # Calculate total weekly hours
    total_weekly_hours = 0
    for day in ['mon', 'tue', 'wed', 'thr', 'fri', 'sat', 'sun']:
        open_time = restaurant.get('diningRoomHours', {}).get(f'{day}Open')
        close_time = restaurant.get('diningRoomHours', {}).get(f'{day}Close')
        if open_time and close_time:
            open_hour, open_minute, _ = map(int, open_time.split(':'))
            close_hour, close_minute, _ = map(int, close_time.split(':'))
            total_weekly_hours += (close_hour * 60 + close_minute - open_hour * 60 - open_minute) / 60

    result = (restaurant_id, store_id, city, state, postal_code, latitude, longitude, status,
              has_breakfast, has_delivery, has_dine_in, has_drive_thru, has_mobile_ordering, has_take_out,
              pos_vendor, total_weekly_hours)

    return result


def simple_restaurants(restaurants):
    """
    Return a list of simple_restaurant tuples for each restaurant in the list.

    Args:
        restaurants (list): A list of dictionaries representing restaurants.

    Returns:
        list: A list of simple_restaurant tuples.
    """

    result = [simple_restaurant(restaurant) for restaurant in restaurants]

    return [restaurant for restaurant in result if restaurant is not None]


async def upload_to_db(restaurants=None, menu_items=None, item_info=None):
    """
    Supply a CSV filename to any parameter. Upload the CSV to the respective table in the database.
    """
    postgres_password = os.environ.get("POSTGRES_PASSWORD", "postgres123")

    batch_size = 100_000

    conn = await asyncpg.connect('postgresql://localhost:5432', user='postgres', password=postgres_password, database='inflation')

    async with conn.transaction():
        if menu_items:
            file = csv.reader(open(menu_items, 'r', newline=''))
            # skip the header
            next(file)

            batch = []
            for row in file:
                correct_row = [int(row[0]), row[1], row[2] == True, float(row[3]), float(row[4]), float(row[5]), float(row[6]), datetime.strptime(row[7], '%Y-%m-%d')]
                batch.append(correct_row)
                if len(batch) == batch_size:
                    await conn.executemany('INSERT INTO bk_menuitems (store_id, item_id, isAvailable, price_min, price_max, price_default, avg_calories, created_date) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)', batch)
                    batch = []
                    print(f"Inserted {batch_size} rows")

            if batch:
                await conn.executemany('INSERT INTO bk_menuitems (store_id, item_id, isAvailable, price_min, price_max, price_default, avg_calories, created_date) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)', batch)

# Example usage
# await upload_to_db(restaurants='restaurants.csv', menu_items='menu_items.csv', item_info='item_info.csv')




def menu_items_update():
    """
    Find the most recent restaurants list in the Temp folder.  Use the store ids to get the most recent menu items from the BK API.
    Save the menu items to a new CSV file in the Temp folder.
    """

    # Find the most recent restaurants list in the Temp folder
    files = os.listdir('Temp')
    files = [f for f in files if 'bk_restaurants' in f and f.endswith('.csv')]
    files.sort(reverse=True)
    if len(files) == 0:
        print("No restaurants files found")
        return
    restaurants_file = files[0]

    # Read the store ids from the restaurants file
    store_ids = set()
    filename = f'Temp{os.sep}{restaurants_file}'
    with open(filename, 'r', newline='') as file:
        reader = csv.reader(file)
        next(reader)
        for row in reader:
            store_ids.add(row[1])


    write_menu_items_to_csv(list(store_ids))

    return filename



if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Harvest Burger King data')
    parser.add_argument('--all', action='store_true', help='Get menu items, restaurants, and item info')
    parser.add_argument('--menuitems_only', action='store_true', help='Just get menu items')
    parser.add_argument("--upload", action="store_true", help="Upload the data to the database")
    args = parser.parse_args()

    if args.all:
        whole_harvest(upload=args.upload)
    elif args.menuitems_only:
        filename = menu_items_update()
        asyncio.run(upload_to_db(menu_items=filename))
    else:
        print("No arguments given.  Use --menuitems_only to harvest the data or --all to update all information.")