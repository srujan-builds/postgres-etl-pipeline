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

    try:
        print("Startin the pipeline run")
        load_info = pipeline.run(sample_data)
        print("Pipeline run completed successfully")
        print(load_info)
    except Exception as e:
        print(f"Pipeline Failure: {e}")

if __name__ == "__main__":
    main()

