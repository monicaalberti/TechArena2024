
import json
import pandas as pd
from os.path import abspath, join


def load_json(path):
    return json.load(open(path, encoding='utf-8'))


def save_json(path, data):
    with open(path, 'w', encoding='utf-8') as out:
        json.dump(data, out, ensure_ascii=False, indent=4)


def load_solution(path):
    # Loads a solution from a json file to a pandas DataFrame.
    return pd.read_json(path)


def save_solution(solution, path):
    # Saves a solution into a json file.
    if isinstance(solution, pd.DataFrame):
        solution = solution.to_dict('records')
    return save_json(path, solution)


def load_problem_data(path=None):
    if path is None:
        path = './data/'

    # LOAD DEMAND
    demand = pd.read_csv('./tech_arena_24_phase_1/data/demand.csv')    
    
    # LOAD DATACENTERS DATA
    datacenters = pd.read_csv('./tech_arena_24_phase_1/data/datacenters.csv')
    
    # LOAD SERVERS DATA
    servers = pd.read_csv('./tech_arena_24_phase_1/data/servers.csv')
    
    # LOAD SELLING PRICES DATA
    selling_prices = pd.read_csv('./tech_arena_24_phase_1/data/selling_prices.csv')
    return demand, datacenters, servers, selling_prices


def make_decision(time_step, datacenters, servers, demand, selling_prices):
    actions = []
    
    # Track deployed servers and their states over time
    deployed_servers = []  # This would ideally be a global variable or passed into the function

    for dc in datacenters.itertuples():
        # Step 1: Buy servers if there's demand and capacity
        for server in servers.itertuples():
            # Check demand for the current data center and server generation
            relevant_demand = demand[(demand['time_step'] == time_step)] 
            """& 
                                    (demand['datacenter_id'] == dc.datacenter_id) & 
                                    (demand['server_generation'] == server.server_generation)]"""
            
            # Buy new servers if demand exists and data center has capacity
            if not relevant_demand.empty and dc.slots_capacity >= server.slots_size:
                action = {
                    "time_step": time_step,
                    "datacenter_id": dc.datacenter_id,
                    "server_generation": server.server_generation,
                    "server_id": f"server_{time_step}_{dc.datacenter_id}_{server.server_generation}",
                    "action": "buy"
                }
                actions.append(action)
                
                # Update the capacity of the data center (assuming it reduces after each buy)
                dc.slots_capacity -= int(server.slots_size)  # Reducing available slots

                # Add the server to the deployed list for future tracking
                deployed_servers.append({
                    "server_id": action["server_id"],
                    "datacenter_id": dc.datacenter_id,
                    "generation": server.server_generation,
                    "slots_size": server.slots_size,
                    "lifespan": server.Lifespan,  # Track server's lifespan
                    "time_step_bought": time_step
                })

        # Step 2: Hold and Move servers
        for deployed_server in deployed_servers:
            # Check if server belongs to this data center and is not yet dismissed
            if deployed_server["datacenter_id"] == dc.datacenter_id:
                # Calculate the remaining lifespan
                remaining_lifespan = deployed_server["lifespan"] - (time_step - deployed_server["time_step_bought"])
                
                # Continue holding servers as long as there's demand and lifespan is positive
                relevant_demand = demand[(demand['time_step'] == time_step) & 
                                        (demand['datacenter_id'] == dc.datacenter_id) & 
                                        (demand['server_generation'] == deployed_server["generation"])]
                
                if remaining_lifespan > 0 and not relevant_demand.empty:
                    # Server can still be held to meet demand
                    action = {
                        "time_step": time_step,
                        "datacenter_id": dc.datacenter_id,
                        "server_generation": deployed_server["generation"],
                        "server_id": deployed_server["server_id"],
                        "action": "hold"
                    }
                    actions.append(action)
                else:
                    # If demand exists elsewhere, consider moving the server
                    for target_dc in datacenters.itertuples():
                        if target_dc.datacenter_id != dc.datacenter_id and target_dc.slots_capacity >= deployed_server["slots_size"]:
                            target_demand = demand[(demand['time_step'] == time_step) & 
                                                (demand['datacenter_id'] == target_dc.datacenter_id) & 
                                                (demand['server_generation'] == deployed_server["generation"])]
                            if not target_demand.empty:
                                action = {
                                    "time_step": time_step,
                                    "datacenter_id": target_dc.datacenter_id,
                                    "server_generation": deployed_server["generation"],
                                    "server_id": deployed_server["server_id"],
                                    "action": "move",
                                    "from_datacenter_id": dc.datacenter_id
                                }
                                actions.append(action)
                                
                                # Update the data center capacities
                                dc.slots_capacity += deployed_server["slots_size"]
                                target_dc.slots_capacity -= deployed_server["slots_size"]
                                
                                # Move the server to the new data center
                                deployed_server["datacenter_id"] = target_dc.datacenter_id
                                break

        # Step 3: Dismiss servers when their lifespan is over or if not profitable
        for deployed_server in deployed_servers:
            if deployed_server["datacenter_id"] == dc.datacenter_id:
                remaining_lifespan = deployed_server["lifespan"] - (time_step - deployed_server["time_step_bought"])
                
                if remaining_lifespan <= 0:
                    # Dismiss the server if its lifespan is over
                    action = {
                        "time_step": time_step,
                        "datacenter_id": dc.datacenter_id,
                        "server_generation": deployed_server["generation"],
                        "server_id": deployed_server["server_id"],
                        "action": "dismiss"
                    }
                    actions.append(action)
                    
                    # Free up capacity in the data center
                    dc.slots_capacity += deployed_server["slots_size"]
                    
                    # Remove the server from the deployed list
                    deployed_servers.remove(deployed_server)
                
                else:
                    # Check if selling is profitable based on current selling prices
                    sell_price = selling_prices[(selling_prices['server_generation'] == deployed_server["generation"]) &
                                                (selling_prices['time_step'] == time_step)]
                    if not sell_price.empty and sell_price['price'].values[0] > 0:
                        action = {
                            "time_step": time_step,
                            "datacenter_id": dc.datacenter_id,
                            "server_generation": deployed_server["generation"],
                            "server_id": deployed_server["server_id"],
                            "action": "sell"
                        }
                        actions.append(action)
                        
                        # Free up capacity in the data center
                        dc.slots_capacity += deployed_server["slots_size"]
                        
                        # Remove the server from the deployed list
                        deployed_servers.remove(deployed_server)

                print (actions)
                return actions



if __name__ == '__main__':

    # load problem data
    problem = load_problem_data()

    final_solution = []
    for t in range (0, 168):
        solution = make_decision(t, problem[1], problem[2], problem[0], problem[3])
        final_solution.append(solution)
    save_solution(solution, "./solutions01.json")




    # Load solution
    path = './data/solutions01.json'

    solution = load_solution(path)

   # print(solution)


    # Save solution
    # path = './data/solution_example_test.json'
    # save_solution(solution, path)

    
    
