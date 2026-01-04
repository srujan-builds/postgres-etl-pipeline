import dlt
import requests
from dotenv import load_dotenv

load_dotenv()

@dlt.resource(table_name='fakestore')
def sample_data():
    url = "https://fakestoreapi.com/products"
    response = requests.get(url)
    yield response.json()


def main():
    pipeline = dlt.pipeline(
        pipeline_name="dlt_pipeline",
        destination="postgres",
        dataset_name="demodb"
    )

    load_info = pipeline.run(sample_data)
    
    print(load_info)

if __name__ == "__main__":
    main()

