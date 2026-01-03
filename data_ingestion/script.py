import requests
import psycopg2

def get_data():
    url = "https://fakestoreapi.com/products"
    response = requests.get(url)
    return response.json()

def transform(raw_data):
    for data in raw_data:
        yield {
            "id": data.get("id"),
            "title": data.get("title"),
            "category": data.get("category"),
            "rating": data.get("rating")["rate"],
            "price": data.get("price")
        }

def push_data_to_postgres(data_generator):

    try:
        # connection to postgres db
        conn = psycopg2.connect(
            database = "demodb", 
            user = "postgres", 
            host= 'postgres-container',
            password = "demopassword"
        )

        # query to execute
        sql_query = """
            INSERT INTO products (id, title, category, rating, price)
            VALUES (%s, %s, %s, %s, %s)
        """

        # Open a cursor to perform database operations
        cursor = conn.cursor()

        # iterating through generator
        for data in data_generator:
            data_tuple = (
                data.get('id'),
                data.get('title'),
                data.get('category'),
                data.get('rating'),
                data.get('price')
            )
            
            # execute query on db
            cursor.execute(sql_query, data_tuple)

        # Make the changes to the database persistent
        conn.commit()
        print("Data successfully committed to DB.")
        
    except Exception as e:
        print(f"Error occurred: {e}")

        # roll-back changes
        conn.rollback()

    finally:
        cursor.close()
        conn.close()
        print("Connection to DB closed")
        


if __name__ == "__main__":
    raw_data = get_data()
    transformed_data = transform(raw_data)
    push_data_to_postgres(transformed_data)