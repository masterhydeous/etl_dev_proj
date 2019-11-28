import re
class EtlUtil:

    def get_component(self,location, component_type):
        for component in location[0].raw['address_components']:
            if component_type in component['types'] and component['long_name']:
                return component['long_name']
