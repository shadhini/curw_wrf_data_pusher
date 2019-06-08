#from db_adapter.curw_fcst.models import create_curw_fcst_db
#from db_adapter.curw_fcst.models import Run, Data, Source, Variable, Unit, Station

#print("Create curw_fcst schema")

#print(create_curw_fcst_db())

#from db_adapter.curw_obs.models import create_curw_obs_db
#from db_adapter.curw_obs.models import Run, Data, Source, Variable, Unit, Station

#print("Create curw_obs schema")

#print(create_curw_obs_db())


from db_adapter.curw_sim.models import create_curw_sim_db
from db_adapter.curw_sim.models import Run, Data, GridMap

print("Create curw_sim schema")

print(create_curw_sim_db())
