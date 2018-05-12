# multiple documents local
import csv
import sys
import re
import random
import binascii
import numpy as np

# reading csv file
list1=[]
with open("/home/rsd352/Articles.csv","rU") as csvfile:
   spamreader = csv.reader(csvfile, delimiter=",", quotechar='"')
   for row in spamreader:
		print("---->")
		print ''.join(row[0])
		list1.append(''.join(row[0]))
		print("<----")

doc_list=sc.parallelize(list1[1:])

d_no=doc_list.count()
# d_no=100
print(d_no)
doc_list_zip=doc_list.zipWithIndex().map(lambda x: (x[1],x[0]))

# upper limit for random integer
max_random=500

# total number of permutations
max_perm=100

a=[]
b=[]

for i in range(0,max_perm):
    a.append(random.randint(0,max_random))

for i in range(0,max_perm):
    b.append(random.randint(0,max_random))

# prime number to perform modulo operation
prime=1000000007

min_docs=[]

def shingles(word,n):
	word0=re.sub("\s\s+"," ",word)
	word1=word0.replace("\n","")
	# print(word1)
	return [word1[i:i + n] for i in range(len(word1) - n + 1)]

for d in range(0,d_no):
	d1=doc_list_zip.lookup(d)
	# print(d1)
	d2=sc.parallelize(d1)
	d3=d2.flatMap(lambda x: shingles(x,5))
	# d3.collect()
	d31=d3.distinct()
	d4=d31.zipWithIndex().map(lambda x: (x[1],x[0]))
	# d4.collect()
	print("-------------------")
	s_no=d4.count()
	print(s_no)
	min_no=[]
	# minhashing	
	for i in range(0,max_perm):
		hashv=d4.map(lambda (x,y): ((a[i]*(binascii.crc32(str(y)) & 0xffffffff)+b[i])%prime)).min()
		# print(hashv)
		min_no.append(hashv)	
	print(min_no)
	print("------------------")
	min_docs.append(min_no);

print(min_docs)

rdd_min=sc.parallelize(min_docs)


rdd_zip=rdd_min.zipWithIndex().map(lambda (x,y):(y,x))


def myDist(l1,l2):
	total=len(l1)
	temp=0
	for i in range(total):
		temp+=(l1[i]-l2[i])**2
	return temp**(1.0/2.0)

def div(l,val):
	temp=[]
	for i in range(0,len(l)):
		temp.append(l[i]/val)
	return temp

def closestPoint(p, centers):
    bestIndex = -1
    closest = sys.maxint
    for i in range(len(centers)):
        tempDist = myDist(p,centers[i])
        if tempDist < closest:
            closest = tempDist
            bestIndex = i
    print(bestIndex)
    return bestIndex


def add_list(l1,l2):
	tot=[]
	for i in range(0,len(l1)):
		tot.append(l1[i]+l2[i])
	return tot


k=4
cent_pts = rdd_min.takeSample(False, k, 1)
print(cent_pts)

for z in range(0,10):
	abc=rdd_min.map(lambda p: (closestPoint(p,cent_pts),(p,1)))
	#abc.collect()
	# rdd_new=abc.reduceByKey(lambda p1_c1,p2_c2: (p1_c1[0]+p2_c2[0],p1_c1[1]+p2_c2[1]))
	rdd_new=abc.reduceByKey(lambda p1_c1,p2_c2: (add_list(p1_c1[0],p2_c2[0]),p1_c1[1]+p2_c2[1]))
	#rdd_new.collect()
	newPoints = rdd_new.map(lambda st: (st[0],div(st[1][0],st[1][1]))).collect()
	for (iK, p) in newPoints:
		cent_pts[iK] = p


len(cent_pts[0])
print("------------")
print(cent_pts)


for i in range(0,d_no):
	dist=0
	min=sys.maxint
	pos=-1
	for j in range(0,k):
		dist=myDist(cent_pts[j],rdd_zip.lookup(i)[0])
		print(dist)
		if dist<min:
			min=dist
			pos=j
			print("pos ",pos)
	# cluster_list.append(pos)
	print("Document ",i," assigned to cluster ",pos)