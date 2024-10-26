def http_transport(encoded_span):
    # Validate input
    if not isinstance(encoded_span, (bytes, bytearray)):
        logging.error("Invalid input: 'encoded_span' must be bytes.")
        return
 
    try:
        # Use HTTPS for security
        response = requests.post(
            'https://servicebridge:80/zipkin',  # Change to HTTPS
            data=encoded_span,
            headers={'Content-Type': 'application/x-thrift'},
            timeout=5  # Set a timeout
        )
        # Handle response status
        if response.status_code != 200:
            logging.error(f"Failed to send data: {response.status_code} - {response.text}")
 
    except requests.RequestException as e:
        logging.error("An error occurred while sending the request.")
        logging.debug(f"Error details: {e}")  # Log details in debug level
