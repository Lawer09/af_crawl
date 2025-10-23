
from model.af_data import AfAppDataDAO
from model.cookie import CookieDAO
from model.user_app_data import UserAppDataDAO

UserAppDataDAO.init_table()
AfAppDataDAO.init_table()
CookieDAO._init_table()