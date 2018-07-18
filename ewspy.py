## import core modules
import re
import lxml
import sys
import logging
from datetime import datetime

## import second-party modules
### for parsing html in the body of emails
import html2text

### for dataframes
import pandas as pd

### for receiving and sending SOAP messages
from requests import Session
from requests_ntlm import HttpNtlmAuth
from zeep import Client
from zeep import xsd
from zeep.plugins import HistoryPlugin
from zeep.transports import Transport

_p=print


# In[ ]: 

class EWS_Client:  

    # set logger to new logger with module's name
    logger = logging.getLogger(__name__)

    # set logger level
    logger.setLevel(logging.INFO)

    def __repr__(
                self
    ):
        return 'EWSClient for username {0}'.format(self.username)

    def __init__(
                self,
                username,
                password,
                find_items_basepoint='Beginning',
                wsdl='Services.wsdl',
                timeout=120,
                max_folder_items_per_find_item_query=1000,
                max_items_per_get_item_query=1000,
                logger=None,
    ):
        self.username=username
        self.password=password
        self.find_items_basepoint=find_items_basepoint
        self.wsdl=wsdl
        self.timeout=timeout
        self.max_folder_items_per_find_item_query=max_folder_items_per_find_item_query
        self.max_items_per_get_item_query=max_items_per_get_item_query

        self.session = Session()
        self.session.auth = HttpNtlmAuth(self.username, self.password)
        self.history = HistoryPlugin()
        self.client = Client(self.wsdl, transport=Transport(session=self.session),plugins=[self.history])
        self.client.options(timeout=120)

        # if logger is provided
        if (bool(logger)):

            # set logger to what was provided
            EWS_Client.logger = logger


    def ews_exception(ews_function, *args, **kwargs):

        def _f_(*args, **kwargs):
            try:
                return ews_function(*args)
            except KeyError as _e:
                EWS_Client.logger.exception('KeyError'+'\n')
                EWS_Client.logger.exception(ews_function)
                EWS_Client.logger.exception(_e)
                return None
            except AttributeError as _e:
                EWS_Client.logger.exception('AttributeError'+'\n')
                EWS_Client.logger.exception(ews_function)
                EWS_Client.logger.exception(_e)
                return None
            except AssertionError as _e:
                EWS_Client.logger.exception('AssertionError'+'\n')
                EWS_Client.logger.exception(ews_function)
#                 EWS_Client.logger.exception(*args)
                EWS_Client.logger.exception(_e)
                return None

        return _f_

    @staticmethod
    @ews_exception
    def get_attribute_from_EWS_response(search_path, tree):
        """
        Returns the value of an attribute in an EWS XML response.  
        The value returned could be an end node or a non-end node in an EWS XML response

        Parameters
        ----------

        - search_path: list containing for traversing the node
        - tree: EWS XML tree

        Notes
        -----

        Method is decorated by @ews_exception. This implies that if the XML tree runs out before 
        the search path is fully traversed or if the node does exist in the expected search path,
        then None is returned.       
        """
        assert tree != None

        node=tree[search_path[0]]

        # check if node is None - that is the tree ran out before the path could be traversed
        if node:
            if len(search_path) == 1:
                return node
            else:
                return EWS_Client.get_attribute_from_EWS_response(search_path[1:], node)
        # the tree did run out; return None
        else:
            return None


    @ews_exception
    def get_folder(self, folder_type, folder_id):
        request = {folder_type : {'Id': folder_id}}

        _response = self.client.service.GetFolder( 
            FolderShape='Default',
            FolderIds={'_value_1':[request]},
            _soapheaders={'RequestVersion':{'Version':'Exchange2010_SP2'}},
        )

        return _response


    @ews_exception
    def get_subfolders(self, folder_type, folder_id):
        request = {folder_type : {'Id': folder_id}}

        response = self.client.service.FindFolder(
                            Traversal='Shallow', 
                            FolderShape='Default', 
                            ParentFolderIds={'_value_1':[request]},
                            _soapheaders={'RequestVersion':{'Version':'Exchange2010_SP2'}},
        )

        return response

    def get_subfolders_df(self, folder_type, folder_id):
        """
        Returns
        -------
        A Pandas DataFrame that contains the folder and any subfolders with columns:
        - parent_folder_id
        - display_name
        - item_count
        - sub_folder_count

        Parameters
        ----------

        - folder_type: ['FolderId', 'DistinguishedFolderId']
        - folder_id: EWS folder id

        Notes
        -----
        None

        """

        outlook_df = pd.DataFrame([], index=[], columns=['parent_folder_id', 'display_name', 'item_count', 'sub_folder_count'])

        return self.add_subfolders_to_df(folder_type, folder_id, outlook_df)


    def add_subfolders_to_df(self, folder_type, folder_id, outlook_df):
        try:
            # get folder summary data
            _response = self.get_folder('FolderId', folder_id )

            # set up path to extract total items and total number of folders in this response
            starting_folder_id_search_path = ['body', 'ResponseMessages', '_value_1', 0, 'GetFolderResponseMessage', 'Folders', '_value_1', 0, 'Folder', 'FolderId', 'Id']
            starting_folder_parent_folder_count_search_path = ['body', 'ResponseMessages', '_value_1', 0, 'GetFolderResponseMessage', 'Folders', '_value_1', 0, 'Folder', 'ParentFolderId']
            starting_folder_display_name_search_path = ['body', 'ResponseMessages', '_value_1', 0, 'GetFolderResponseMessage', 'Folders', '_value_1', 0, 'Folder', 'DisplayName']
            starting_folder_total_count_search_path = ['body', 'ResponseMessages', '_value_1', 0, 'GetFolderResponseMessage', 'Folders', '_value_1', 0, 'Folder', 'TotalCount']
            starting_folder_child_folder_count_search_path = ['body', 'ResponseMessages', '_value_1', 0, 'GetFolderResponseMessage', 'Folders', '_value_1', 0, 'Folder', 'ChildFolderCount']

            # display what was found
            starting_folder_id = self.get_attribute_from_EWS_response(starting_folder_id_search_path, _response)

            outlook_df.loc[starting_folder_id]=None
            outlook_df.loc[starting_folder_id, 'parent_folder_id'] = folder_id
            outlook_df.loc[starting_folder_id, 'display_name'] = self.get_attribute_from_EWS_response(starting_folder_display_name_search_path, _response)
            outlook_df.loc[starting_folder_id, 'item_count'] = self.get_attribute_from_EWS_response(starting_folder_total_count_search_path, _response)
            outlook_df.loc[starting_folder_id, 'sub_folder_count'] = self.get_attribute_from_EWS_response(starting_folder_child_folder_count_search_path, _response)

            # get sub folder data
            _response = self.get_subfolders(folder_type, folder_id)

            # path to sub folders
            list_of_folders_search_path = ['body', 'ResponseMessages', '_value_1', 0, 'FindFolderResponseMessage', 'RootFolder', 'Folders', '_value_1']

            # get subfolders data
            list_of_folders = self.get_attribute_from_EWS_response(list_of_folders_search_path, _response)

            # search path to sub folder id
            sub_folder_id_search_path = ['Folder', 'FolderId', 'Id']
            sub_folder_display_name_search_path = ['Folder', 'DisplayName']
            sub_folder_total_count_search_path = ['Folder', 'TotalCount']
            sub_folder_child_folder_count_search_path = ['Folder', 'ChildFolderCount']

            # iterate over sub folders
            for folder in list_of_folders:
                try:
                    # get sub folder id
                    sub_folder_id = self.get_attribute_from_EWS_response(sub_folder_id_search_path, folder)

                    outlook_df = self.add_subfolders_to_df('FolderId', sub_folder_id, outlook_df)

                except Exception as e:
                    EWS_Client.logger.exception(e)
                    EWS_Client.logger.exception(folder)
                    pass
            return outlook_df
        except Exception as e:
    #         EWS_Client.logger.exception(self)
            return outlook_df

    @staticmethod
    @ews_exception
    def get_path_for_attribute(attribute):

        _path_dict_ = {
            'ITEM_COUNT':['body', 'ResponseMessages', '_value_1', 0, 'GetFolderResponseMessage', 'Folders', '_value_1', 0, 'Folder', 'TotalCount'],
            'CHILD_FOLDER_COUNT':['body', 'ResponseMessages', '_value_1', 0, 'GetFolderResponseMessage', 'Folders', '_value_1', 0, 'Folder', 'ChildFolderCount'],
            'CHILD_FOLDERS':['body', 'ResponseMessages', '_value_1', 0, 'FindFolderResponseMessage', 'RootFolder', 'Folders', '_value_1'],
            'FROM_EMAIL_ADDRESS':['From', 'Mailbox', 'EmailAddress'],
        }

        return _path_dict_[attribute]


    @ews_exception
    def find_items(
                    self, 
                    folder_type,
                    folder_id,
                    offset,
                    ItemShape='AllProperties',
                    Traversal='Shallow',
                    FractionalPageItemView=None,
                    CalendarView=None,
                    ContactsView=None,
                    GroupBy=None,
                    DistinguishedGroupBy=None,
                    Restriction=None,
                    SortOrder=None,
                    QueryString=None,
                    _soapheaders={'RequestVersion':{'Version':'Exchange2010_SP2'}},
    ):

        _parent_folder_id_xml={'_value_1':[
                                            {folder_type:{'Id':folder_id}}
                                            ]
                              }

        _response = self.client.service.FindItem(  
                                                    ItemShape=ItemShape,
                                                    Traversal=Traversal, 
                                                    IndexedPageItemView={
                                                        'MaxEntriesReturned':self.max_folder_items_per_find_item_query,
                                                        'Offset':offset,
                                                        'BasePoint':self.find_items_basepoint,
                                                    },
                                                    FractionalPageItemView=FractionalPageItemView,
                                                    CalendarView=CalendarView,
                                                    ContactsView=ContactsView,
                                                    GroupBy=GroupBy,
                                                    DistinguishedGroupBy=DistinguishedGroupBy,
                                                    Restriction=Restriction,
                                                    SortOrder=SortOrder,
                                                    ParentFolderIds=_parent_folder_id_xml,
                                                    QueryString=QueryString,
                                                    _soapheaders=_soapheaders,
        )

        return _response


    @ews_exception
    def get_all_items_in_folder(self, folder_type, folder_id, query=None):

        # set up dataframe that will returned
        _index=pd.Index([],name='item_id',dtype=str)

        # figure out how many items in the folder
        _response = self.get_folder(folder_type, folder_id)
        _folder_items_count = EWS_Client.get_attribute_from_EWS_response(EWS_Client.get_path_for_attribute('ITEM_COUNT'), _response)

        # artificially set item count to 100
        # _folder_items_count = 100

        EWS_Client.logger.info('..folder_items_count:{0}'.format(_folder_items_count))

        # if there are no items in the folder
        if not(bool(_folder_items_count)):

            # return None
            raise StopIteration('Empty outlook folder') 

        # go through each session
        # for _beg in list(_entries_item_ids_in_view_beg):
        for _beg in range(0, _folder_items_count, self.max_folder_items_per_find_item_query):

            EWS_Client.logger.info('_'*10)
            EWS_Client.logger.info(_beg,)

            # set up temporary session
            _session = Session()
            _session.auth = HttpNtlmAuth(self.username, self.password)           
            _client = Client(self.wsdl, transport=Transport(session=_session))

            # get the items; most options are defaulted
            _response = self.find_items(folder_type, folder_id, _beg, QueryString=query)

            # close temporary session
            _session.close()

            # get all the items in response, if any
            _items_path = ['body', 'ResponseMessages', '_value_1', 0, 'FindItemResponseMessage', 'RootFolder', 'Items', '_value_1']         
            _items = self.get_attribute_from_EWS_response(_items_path, _response)

            items = pd.DataFrame(index=_index, columns=['date_time_received', 'full_item', 'ews_error'])

            # iterate through each item
            for _item in _items:
                try:

                    # get item id
                    _item_id_path = ['Message', 'ItemId', 'Id']
                    _item_id = self.get_attribute_from_EWS_response(_item_id_path, _item)

                    # make sure something was returned; None should be returned if it is a CalendarItem
                    if _item_id:

                        # get item received item data and path
                        _item_received_dt_tm_search_path = ['Message', 'DateTimeReceived']
                        _item_received_dt_tm = self.get_attribute_from_EWS_response(_item_received_dt_tm_search_path, _item)

                        # add to data frame
                        items.set_value(_item_id, 'date_time_received', _item_received_dt_tm)
                    else:
                        pass
                except Exception as e:
                    pass

            yield(items)


    @ews_exception
    def get_items(self, items):
        """
        Returns
        -------
        - Pandas DataFrame with the full_item and ews_error codes filled in.


        Arguments
        ---------      
        - items: Pandas DataFrame with the item_id as Index, full_item column, and ews_error column

        Notes
        -----
        - Function will overwrite the full_item and ews_error columns.       
        """
        _beg_indices=(pd.Series(range(0, len(items), self.max_items_per_get_item_query))).tolist()
        _end_indices=(pd.Series(range(0, len(items), self.max_items_per_get_item_query))+self.max_items_per_get_item_query).tolist()
        _end_indices[-1]=min(_end_indices[-1],len(items))


        for _beg_index, _end_index in zip(_beg_indices, _end_indices):
            EWS_Client.logger.info('_'*10)
            EWS_Client.logger.info('_beg_index:{0}, _end_index:{1}'.format(_beg_index, _end_index))

            # list of item ids that will be queried for full data
            _item_ids_subset = items.iloc[_beg_index:_end_index].index.tolist()
            _full_item_query=[]

            for _item_id in _item_ids_subset:
                _full_item_query.append({'ItemId':{'Id':_item_id}})

            EWS_Client.logger.info('Session:starting....')
            _session = Session()
            _session.auth = HttpNtlmAuth(self.username, self.password)
            _client = Client(self.wsdl, transport=Transport(session=_session))
            _response = _client.service.GetItem(
                                                ItemShape='AllProperties',
                                                ItemIds={'_value_1':_full_item_query},
                                                _soapheaders={'RequestVersion':{'Version':'Exchange2010_SP2'}},
            )
            _session.close()
            EWS_Client.logger.info('Session:ended')

            _items_in_response = EWS_Client.get_attribute_from_EWS_response(['body', 'ResponseMessages', '_value_1'], _response)

            EWS_Client.logger.info('count of items received:{}'.format(len(_items_in_response)))

            for (_item_response, _item_id) in zip(_items_in_response, _item_ids_subset):
                try:                    
                    _full_email_path = ['GetItemResponseMessage', 'Items', '_value_1', 0, 'Message']
                    _full_email = self.get_attribute_from_EWS_response(_full_email_path, _item_response)

                    if _full_email:

                        # Get the item id. Why? To experiment and see item ids match up
                        _item_id_path = ['GetItemResponseMessage', 'Items', '_value_1', 0, 'Message', 'ItemId', 'Id'] 
                        _item_id_2 = self.get_attribute_from_EWS_response(_item_id_path, _item_response)

                        assert _item_id == _item_id_2                        

                        items.set_value(_item_id, 'full_item', _full_email)

                except TypeError as _te:
                    EWS_Client.logger.exception(_te)
                    items.set_value(_item_id, 'ews_error', 'get_type_error'+str(_te))
                    pass
                except AssertionError as _ae:
                    EWS_Client.logger.exception(_ae)
#                     EWS_Client.logger.exception(_item_response)
                    items.set_value(_item_id, 'ews_error', 'get_assertion_error'+str(_ae))
                    pass

        return items

    @ews_exception
    def convert_id(self, ews_id_from, ews_id_from_format, ews_id_to_format):
        """
        Returns
        -------
        - converts from one EWS Id format to another EWS Id format 


        Arguments
        ---------      
        - ews_id_from: id that needs converting 
        - ews_id_from_format: id format that needs converting
        - ews_id_to_format: id format that to which the id needs to be converted 

        Notes
        -----
        - 
        """

        request_body = [{'AlternateId':{'Id':ews_id_from, 'Mailbox':'dbiswas@morgancreekcap.com', 'Format':ews_id_from_format}}]


        response = self.client.service.ConvertId(
                          DestinationFormat=ews_id_to_format
                        , SourceIds={'_value_1':request_body}
                        , _soapheaders={'RequestVersion':{'Version':'Exchange2010_SP2'}}
        )


        return response
