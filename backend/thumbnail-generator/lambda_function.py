import boto3
import os
from PIL import Image
from io import BytesIO

s3 = boto3.client('s3')

THUMBNAILS_BUCKET = os.environ['THUMBNAILS_BUCKET']
THUMBNAIL_SIZE = (400, 400)  # Width, Height

def lambda_handler(event, context):
    """
    Triggered when image uploaded to originals bucket.
    Creates thumbnail and saves to thumbnails bucket.
    """
    
    try:
        # Get bucket and key
        bucket = event['Records'][0]['s3']['bucket']['name']
        key = event['Records'][0]['s3']['object']['key']
        
        print(f"Creating thumbnail for: {bucket}/{key}")
        
        # Download image from S3
        response = s3.get_object(Bucket=bucket, Key=key)
        image_data = response['Body'].read()
        
        # Open image with Pillow
        image = Image.open(BytesIO(image_data))
        
        # Convert RGBA to RGB if necessary
        if image.mode in ('RGBA', 'LA', 'P'):
            image = image.convert('RGB')
        
        # Create thumbnail (maintains aspect ratio)
        image.thumbnail(THUMBNAIL_SIZE, Image.Resampling.LANCZOS)
        
        # Save to bytes buffer
        buffer = BytesIO()
        image.save(buffer, 'JPEG', quality=85, optimize=True)
        buffer.seek(0)
        
        # Upload thumbnail to S3
        s3.put_object(
            Bucket=THUMBNAILS_BUCKET,
            Key=key,
            Body=buffer,
            ContentType='image/jpeg'
        )
        
        print(f"Thumbnail created successfully: {THUMBNAILS_BUCKET}/{key}")
        
        return {
            'statusCode': 200,
            'body': f'Thumbnail created for {key}'
        }
        
    except Exception as e:
        print(f"Error creating thumbnail: {str(e)}")
        return {
            'statusCode': 500,
            'body': str(e)
        }
        