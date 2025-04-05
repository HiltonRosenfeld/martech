import json

offset_filename = "offset_state"

class OffsetState:

    # Get the stored state values for the comm_by_activity_day
    def get_state(state_name):
    
        with open(f"{offset_filename}.json", 'r') as f:
            state = json.load(f)

        comm_offset_time = state[state_name]['comm_offset_time']
        comm_offset_count = state[state_name]['comm_offset_count']

        print(f"State Offset Time: { comm_offset_time}  State Offset Count: {comm_offset_count}")
        return(comm_offset_time, comm_offset_count)
        


    # Set the state value for the state ""
    def set_state(comm_time_bucket, total_comm_count):
        
        state = {
            "comm_by_activity_day": {
                "comm_offset_time": comm_time_bucket,
                "comm_offset_count": total_comm_count
            }
        }

        with open(f"{offset_filename}.json", 'w') as f:
            json.dump(state, f)        