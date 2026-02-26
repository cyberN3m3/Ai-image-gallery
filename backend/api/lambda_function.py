import json
import boto3
import os
from boto3.dynamodb.conditions import Attr, Key

dynamodb = boto3.resource('dynamodb')
TABLE_NAME = os.environ['DYNAMODB_TABLE']
table = dynamodb.Table(TABLE_NAME)

def lambda_handler(event, context):
    """
    API Gateway handler for searching images.
    
    Endpoints:
    - GET /images - List all images
    - GET /images?search=tag - Search by tag
    - GET /images/{imageId} - Get single image
    """
    
    # Enable CORS
    headers = {
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Headers': 'Content-Type',
        'Access-Control-Allow-Methods': 'GET, POST, OPTIONS'
    }
    
    try:
        http_method = event.get('httpMethod', '')
        path = event.get('path', '')
        query_params = event.get('queryStringParameters') or {}
        
        # Handle OPTIONS for CORS preflight
        if http_method == 'OPTIONS':
            return {
                'statusCode': 200,
                'headers': headers,
                'body': ''
            }
        
        # GET /images - List all or search
        if http_method == 'GET' and path == '/images':
            search_term = query_params.get('search', '').lower()
            
            if search_term:
                # Search by tag
                response = table.scan(
                    FilterExpression=Attr('tags').contains(search_term.title())
                )
            else:
                # Get all images
                response = table.scan()
            
            items = response.get('Items', [])
            
            # Sort by upload date (newest first)
            items.sort(key=lambda x: x.get('uploadDate', ''), reverse=True)
            
            # Convert Decimal to float for JSON serialization
            items = json.loads(json.dumps(items, default=decimal_default))
            
            return {
                'statusCode': 200,
                'headers': headers,
                'body': json.dumps({
                    'images': items,
                    'count': len(items)
                })
            }
        
        # GET /images/{imageId} - Get single image
        elif http_method == 'GET' and path.startswith('/images/'):
            image_id = path.split('/')[-1]
            
            response = table.get_item(Key={'imageId': image_id})
            item = response.get('Item')
            
            if not item:
                return {
                    'statusCode': 404,
                    'headers': headers,
                    'body': json.dumps({'error': 'Image not found'})
                }
            
            item = json.loads(json.dumps(item, default=decimal_default))
            
            return {
                'statusCode': 200,
                'headers': headers,
                'body': json.dumps(item)
            }
        
        # Unknown endpoint
        else:
            return {
                'statusCode': 404,
                'headers': headers,
                'body': json.dumps({'error': 'Endpoint not found'})
            }
    
    except Exception as e:
        print(f"Error: {str(e)}")
        return {
            'statusCode': 500,
            'headers': headers,
            'body': json.dumps({'error': str(e)})
        }

def decimal_default(obj):
    """Helper to convert Decimal to float for JSON"""
    from decimal import Decimal
    if isinstance(obj, Decimal):
        return float(obj)
    raise TypeError