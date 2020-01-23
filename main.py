import pandas as pd
import pyodata
import requests

from airflow.hooks.http_hook import HttpHook
from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class SAPODataOperator(BaseOperator):
    """
    SAPODataOperator for downloading data from SAP OData services and convert it to pandas DataFrame.
    Pushes result to xcom.
    
    :param http_hook: predefined http_hook to use for data transmitting.
        Either `http_hook` or `http_conn_id` needs to be provided.
    :type http_hook: airflow.hooks.http_hook.HttpHook
    :param http_conn_id: connection id from airflow Connections.
        `http_conn_id` will be ignored if `http_hook` is provided.
    :type http_conn_id: str
    :param service_url: URL of the OData service.
        It will replace the `base_url` which was
        defined in `http_hook` or predefined in the connection of `http_conn_id`.
    :type service_url: str
    :param function: name of the function, that provides the data.
    :type function: str
        Either `function` or `entity` needs to be provided.
    :param entity: name of the entity set that provides the data.
    :type entity: str
    :param property: property for data recieving.
        Nullable. If not provided, will be replaced with `entity` value.
    :type property: str
    :param parameters: parameters for the service.
    :type parameters: dict
    """
    
    # template_fields = ('parameters')
    
    ui_color = '#dae2f2'
    ui_fgcolor = '#19477e'
    
    @apply_defaults
    def __init__(self,
                 http_hook=None,
                 http_conn_id=None,
                 service_url=None,
                 function=None,
                 entity=None,
                 property=None,
                 parameters=dict(),
                 *args,
                 **kwargs):
        super().__init__(*args, **kwargs)
        self.http_hook = http_hook
        self.http_conn_id = http_conn_id
        self.service_url = service_url
        self.function = function
        self.entity = entity
        self.property = property
        self.parameters = parameters
        
        if not self.property and not self.function:
            raise AirflowException(f"Either `function` or `entity` needs to be provided.")
        if not self.service_url:
            raise AirflowException(f"`service_url` needs to be provided.")
        if not isinstance(self.parameters, dict):
            raise TypeError(f"`parameters` should be dictionary like {parameter: value}, got {type(self.parameters)}")
    
    def execute(self, context):
        step_msg = None
        result = None
        
        try:
            if self.http_conn_id:
                if self.http_hook and isinstance(self.http_hook, HttpHook):
                    self.log.info("`http_conn_id` is ignored when `http_hook` is provided.")
                else:
                    self.log.info("`http_hook` is not provided or invalid. "
                                  "Trying `http_conn_id` to create HttpHook.")
                    self.http_hook = HttpHook(http_conn_id=self.http_conn_id)
            if not self.http_hook:
                step_msg = "creating http session"
                raise AirflowException("Cannot operate without `http_hook` or `http_conn_id`.")
            else:
                url = requests.urllib3.util.url.parse_url(self.service_url)
                self.http_hook.base_url = f"{url.scheme}://{url.host}" + f"{':' + str(url.port) if url.port else ''}"
            
            with self.http_hook.get_conn() as sess:
                client = pyodata.Client(self.service_url, sess)
                
                if self.function is not None and isinstance(self.function, str):
                    if self.entity is not None:
                        self.log.info("entity is ignored when function is provided.")
                    
                    step_msg = f"preparing data request for function {self.function}"
                    
                    request = client.functions.__getattr__(self.function)
                    if self.parameters:
                        for name, value in self.parameters.items():
                            request = request.parameter(name, value)
                    
                    step_msg = f"sending request and recieving data"
                    
                    request = request.execute()
                    
                    step_msg = f"creating result pandas DataFrame"
                    
                    result = pd.DataFrame.from_dict(request['results'])
                elif self.entity:
                    if not self.property:
                        self.log.info("`property` is not provided, filling with `entity` value")
                        self.property = self.entity
                    
                    step_msg = f"preparing data request for entity set {self.entity}"

                    data = client.entity_sets.__getattr__(self.entity)\
                                .get_entity(**self.parameters).nav(self.property).get_entities()
                    
                    step_msg = f"sending request and recieving data"

                    data = data.execute()
                    
                    step_msg = f"recieving headers"
                    
                    columns = {
                        es.name: es
                        for es in client.schema.entity_sets
                    }[self.property].entity_type.proprties()

                    step_msg = f"creating result pandas DataFrame"

                    result = pd.DataFrame.from_records([{
                        col.name: d.__getattr__(col.name)
                        for col in columns
                    } for d in data])
            
        except Exception as e:
            raise AirflowException(f"Error while step \"{step_msg}\", error: {str(e)}")
        
        return result
