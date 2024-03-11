#!/usr/bin/python3



import ipaddress
import pandas as pd, numpy as np
from netaddr import IPSet as netaddr_IPSet,IPRange as netaddr_IPRange

class IpCoverageCalculator:
    '''Calculates coverage of IPv4 address prefixes within a file'''
    __ip_ranges_ = []
    __ip_set__ = netaddr_IPSet()
    __asn_alloc_ranges = None

   #  def __init_(self):
   #      return self



    #find largest distinct cidr blocks in a list.
    #This can be to assess those within the object or from an entirely separate list.
    def find_cidrs(self,inList):
        ranges = [ipaddress.IPv4Interface(inList[0]).network]
        for cidr in inList[1:]:
            covered = False
            cidrnet = ipaddress.IPv4Interface(cidr).network
            for block in ranges:
                if block.supernet_of(cidrnet):
                    covered = True
                    break
                else:
                    if block.subnet_of(cidrnet):
                        ranges.remove(block)
            if not covered:
                ranges.append(cidrnet)
        return ranges


    #find the most consolidated CIDR blocks that are allocated to the same origin AS (ASN)
    def find_asn_allocs(self,inlist):
        pass#placeholder


    #overloaded constructor
    def __init__(self,inList=[]):
        if inList == []:
            self.__ip_ranges_ = []
        else:
            self.__ip_ranges_ = self.find_cidrs(inList)
        return 


    #Add previously rejected ranges to the coverage results/computation.
    # NOTE: This method does not currently perform checks to ensure proper 
    # file formatting of otherList's source or that geofeed entries to be added meet other validation checks etc.
    def __find_added_cidrs_(self,otherList):
        ranges = self.get_ip_ranges()
        for cidr in otherList:
            covered = False
            cidrnet = ipaddress.IPv4Interface(cidr).network
            for block in ranges:
                if block.supernet_of(cidrnet):
                    covered = True
                    break
                else:
                    if block.subnet_of(cidrnet):
                        ranges.remove(block)
            if not covered:
                ranges.append(cidrnet)
        self.__ip_ranges_ = ranges
        return


    def get_ip_ranges(self):
        return self.__ip_ranges_


    #computes the total number of IPv4 addresses covered within object's __ip_ranges_ attribute
    def get_num_addresses(self):
        ranges = self.get_ip_ranges()
        if ranges == []:
            return 0
        total = 0
        for cidr in ranges:
            total += cidr.num_addresses
        return total




    # Determine the overlap between two IpCoverageCalculator objects
    def compare_ranges(self,other):
        overlapping = []
        if isinstance(other, IpCoverageCalculator):
            if self.ip_ranges==[] or other.get_ip_ranges() == []:
                return overlapping
            #CIDR blocks that overlap are either cover the same IP addresses,
            # or one of the ranges is a proper subset of the other (w.l.o.g.)
            for entry in self.__ip_ranges_:
                for ipnet in other.get_ip_ranges():
                    if entry.supernet_of(ipnet):
                        overlapping.append(ipnet)
                    else:
                        if entry.subnet_of(ipnet):
                            overlapping.append(entry)
                    # overlap = set(entry.hosts()).intersection(set(ipnet.hosts()))
                    # overlaps = overlaps.union(overlap)
            return overlapping

    

    def compare_ip_asn_ranges(self,other):
        pass#placeholder

    # TODO DEFINE THE CHECKS.
    # Wrapper for __find_added_cidrs_(). 
    # Performs validity tests before allowing entries within otherList 
    # to be passed into the private updater function. 
    def add_rejects(self,otherList):
        pass#placeholder



