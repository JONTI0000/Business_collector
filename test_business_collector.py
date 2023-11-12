import pytest
import unittest
from unittest.mock import patch, MagicMock,Mock
from business_collector import BusinessCollector

class TestBusinessCollector(unittest.TestCase):

    @pytest.fixture
    def business_collector_instance(self):
        key = "7e7266ba-8be3-4cbf-a868-3d9a27167920"
        ll = "@51.36,-0.14670,14z"
        catergory = "personal_services"
        town_center = "carshalton"
        search_term = "photography services"
        return BusinessCollector(api_key=key, catergory=catergory, town_center=town_center, search_term=search_term, ll=ll, count=1)
    
    @patch('http.client.HTTPSConnection')
    def test_test_connection_success(self, mock_https_connection,business_collector_instance):
        # Create a mock connection
        conn_mock = MagicMock()
        conn_mock.status = 404
        conn_mock.reason ="error"

        # Configure the mock to return the mock connection
        mock_https_connection.return_value = conn_mock

        # Call the method you want to test
        with pytest.raises(Exception):
            business_collector_instance.test_connection(conn_mock)