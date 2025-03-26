import pandas as pd
import boto3
from content_analytics.utils.config import settings
from content_analytics.utils.data_model import MediaEvent

def __analyze_parquet_files():
    # Initialize S3 client
    s3_client = boto3.client(
        "s3",
        aws_access_key_id=settings.s3_access_key_id,
        aws_secret_access_key=settings.s3_secret_access_key,
        endpoint_url=settings.s3_endpoint_url,
        region_name=settings.s3_region,
        use_ssl=settings.s3_use_ssl,
        config=boto3.session.Config(
            signature_version="s3v4", s3={"addressing_style": "path"}
        ),
    )

    # List objects in the bucket
    bucket = "content-analytics"
    prefix = settings.storage_base_path
    
    response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)

    if 'Contents' not in response:
        return {"error": "No files found in bucket"}

    results = []
    
    # Process each parquet file
    for obj in response['Contents']:
        if not obj['Key'].endswith('.parquet'):
            continue
            
        file_results = {}
        file_results["file"] = obj['Key']
        
        try:
            # Get the object
            response = s3_client.get_object(Bucket=bucket, Key=obj['Key'])
            df = pd.read_parquet(response['Body'])
            
            # Collect basic info
            file_results["shape"] = df.shape
            file_results["columns"] = df.columns.tolist()
            
            # Collect statistics
            file_results["event_type_counts"] = df['event_type'].value_counts().to_dict()
            file_results["content_type_counts"] = df['content_type'].value_counts().to_dict()
            
        except Exception as e:
            file_results["error"] = str(e)
            
        results.append(file_results)
    

    result_length = len(results)

    return result_length, results

def test_parquet_storage():
    result_length, results = __analyze_parquet_files()
    assert result_length > 0
    assert len(results) == result_length
    for result in results:
        print(result)
        assert result["error"] is None

# If ran as script
if __name__ == "__main__":
    result_length, results = __analyze_parquet_files()
    print(f"Found {result_length} files")
    for result in results:
        print(result)

    
