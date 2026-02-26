import json
import boto3
import os
from datetime import datetime
from decimal import Decimal
from uuid import uuid4

# Initialize AWS clients
s3 = boto3.client('s3')
rekognition = boto3.client('rekognition')
dynamodb = boto3.resource('dynamodb')

# Environment variables
TABLE_NAME = os.environ['DYNAMODB_TABLE']
THUMBNAILS_BUCKET = os.environ['THUMBNAILS_BUCKET']

# DynamoDB table
table = dynamodb.Table(TABLE_NAME)

def lambda_handler(event, context):
    """
    Triggered when image is uploaded to S3.
    Analyzes image with Rekognition and stores metadata in DynamoDB.
    """
    
    try:
        # Get bucket and key from S3 event
        bucket = event['Records'][0]['s3']['bucket']['name']
        key = event['Records'][0]['s3']['object']['key']
        
        print(f"Processing image: {bucket}/{key}")
        
        # Get image metadata from S3
        s3_object = s3.head_object(Bucket=bucket, Key=key)
        file_size = s3_object['ContentLength']
        
        # Generate unique image ID
        image_id = str(uuid4())
        
        # Detect labels (objects, scenes, concepts)
        labels_response = rekognition.detect_labels(
            Image={'S3Object': {'Bucket': bucket, 'Name': key}},
            MaxLabels=20,
            MinConfidence=70
        )
        
        # Extract labels
        labels = []
        tags = []
        for label in labels_response['Labels']:
            labels.append({
                'Name': label['Name'],
                'Confidence': Decimal(str(round(label['Confidence'], 2)))
            })
            tags.append(label['Name'])
        
        # Detect text in image
        text_response = rekognition.detect_text(
            Image={'S3Object': {'Bucket': bucket, 'Name': key}}
        )
        
        detected_text = []
        for text in text_response['TextDetections']:
            if text['Type'] == 'LINE':  # Only get full lines, not individual words
                detected_text.append(text['DetectedText'])
        
        # Detect faces
        faces_response = rekognition.detect_faces(
            Image={'S3Object': {'Bucket': bucket, 'Name': key}},
            Attributes=['ALL']
        )
        
        face_count = len(faces_response['FaceDetails'])
        
        # Content moderation (check for inappropriate content)
        moderation_response = rekognition.detect_moderation_labels(
            Image={'S3Object': {'Bucket': bucket, 'Name': key}},
            MinConfidence=60
        )
        
        moderation_flags = [label['Name'] for label in moderation_response['ModerationLabels']]
        
        # Generate URLs
        original_url = f"https://{bucket}.s3.amazonaws.com/{key}"
        thumbnail_url = f"https://{THUMBNAILS_BUCKET}.s3.amazonaws.com/{key}"
        
        # Store in DynamoDB
        item = {
            'imageId': image_id,
            'originalUrl': original_url,
            'thumbnailUrl': thumbnail_url,
            'fileName': key,
            'uploadDate': datetime.utcnow().isoformat(),
            'fileSize': file_size,
            'tags': tags,
            'labels': labels,
            'text': detected_text,
            'faces': face_count,
            'moderationFlags': moderation_flags,
            'processed': True
        }
        
        table.put_item(Item=item)
        
        print(f"Successfully processed {image_id}: {len(labels)} labels, {len(detected_text)} text items, {face_count} faces")
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Image processed successfully',
                'imageId': image_id,
                'tags': tags
            })
        }
        
    except Exception as e:
        print(f"Error processing image: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }