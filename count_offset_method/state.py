import json
import os

offset_filename = "state_offset"

class OffsetState:

    # define init function
    def __init__(self):
        pass


    # Get the stored state values for the comm_by_activity_day
    def get_state(state_name):

        # Check if the file exists
        if not os.path.exists(f"{offset_filename}.json"):
            return(0,0)
        
        with open(f"{offset_filename}.json", 'r') as f:
            state = json.load(f)

        # Check that the state exists
        if state_name not in state:
            print(f"State {state_name} not found")
            return(0,0)
    
        comm_offset_time = state[state_name]['comm_offset_time']
        comm_offset_count = state[state_name]['comm_offset_count']

        print(f"State Offset Time: { comm_offset_time}  State Offset Count: {comm_offset_count}")
        return(comm_offset_time, comm_offset_count)
        

    # Set the state value for the state ""
    def set_state(state_name, comm_time_bucket, total_comm_count):
        
        state = {
            state_name: {
                "comm_offset_time": comm_time_bucket,
                "comm_offset_count": total_comm_count
            }
        }

        with open(f"{offset_filename}.json", 'w') as f:
            json.dump(state, f)