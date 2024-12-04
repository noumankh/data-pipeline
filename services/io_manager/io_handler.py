from abc import ABC, abstractmethod


class IOHandler(ABC):
    """
    Abstract base class for I/O handlers.
    Defines a standard interface for all I/O operations.
    """

    @abstractmethod
    def read(self, *args, **kwargs):
        """
        Read data from the specified source.

        Args:
            *args: Positional arguments for the specific implementation.
            **kwargs: Keyword arguments for the specific implementation.

        Returns:
            The read data (format depends on the specific implementation).
        """
        pass

    @abstractmethod
    def write(self, *args, **kwargs):
        """
        Write data to the specified destination.

        Args:
            *args: Positional arguments for the specific implementation.
            **kwargs: Keyword arguments for the specific implementation.

        Returns:
            None
        """
        pass

    def validate_source(self, source: str):
        """
        Validate the source (e.g., file path, Kafka topic, etc.).

        Args:
            source (str): The source to validate.

        Raises:
            ValueError: If the source is invalid.
        """
        if not source or not isinstance(source, str):
            raise ValueError(f"Invalid source: {source}")

    def validate_destination(self, destination: str):
        """
        Validate the destination (e.g., file path, Kafka topic, etc.).

        Args:
            destination (str): The destination to validate.

        Raises:
            ValueError: If the destination is invalid.
        """
        if not destination or not isinstance(destination, str):
            raise ValueError(f"Invalid destination: {destination}")

    @abstractmethod
    def clear(self, *args, **kwargs):
        """
        Write data to the specified destination.

        Args:
            *args: Positional arguments for the specific implementation.
            **kwargs: Keyword arguments for the specific implementation.

        Returns:
            None
        """
        pass

    @abstractmethod
    def read_all(self, *args, **kwargs):
        """
        Read all data from the specified source.

        Args:
            *args: Positional arguments for the specific implementation.
            **kwargs: Keyword arguments for the specific implementation.

        Returns:
            None
        """
        pass