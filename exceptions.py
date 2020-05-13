class GettingCPUDataException(Exception):
    """
    Creates a Unique Exception for CPU errors
    """
    def __init__(self, instance):
        super().__init__(f"Error occurred while trying to fetch cpu results for instance '{instance.id}'")
