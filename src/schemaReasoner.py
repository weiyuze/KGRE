import sys
import re
from pyspark import SparkContext, SparkConf, StorageLevel
import datetime

def toMap(list_t):
	return dict((i[0], i[1]) for i in list_t)

def main(schemaFile, outputPath, ruleFile):
	conf = SparkConf().setAppName("KGReasoner")
	#conf = SparkConf().setAppName("KGReasoner")
	sc = SparkContext(conf = conf)
	parallism = 10
	storagelevel = StorageLevel.MEMORY_ONLY

	schema = sc.parallelize([])
	with open(schemaFile) as sf:
		for line in sf:
			if(line.strip() != '' and re.match("#", line.strip()) == None):
				schema = schema.union(sc.textFile(line.strip()).map(lambda t: tuple(t.split('\t'))))
	rule = {}
	with open(ruleFile) as rulefile:
		for line in rulefile:
			items = line.strip().split(',')
			rule[items[0]] = items[1]
	rules = sc.broadcast(rule)
	
	#process schema
	restri_first 		= 	sc.broadcast(\
								toMap(\
								schema.filter(lambda t: t[1] == rules.value["OWL_FIRST"])\
								  	  .map(lambda t: (t[0], t[2])).collect()\
								))
	restri_rest 		= 	sc.broadcast(\
								toMap(\
								schema.filter(lambda t: t[1] == rules.value["OWL_REST"])\
								 	  .map(lambda t: (t[0], t[2])).collect()\
								))
	onProp 				= 	toMap(\
								schema.filter(lambda t: t[1] == rules.value["OWL_ON_PROPERTY"])\
					  				  .map(lambda t: (t[0], t[2])).collect()\
								)
	subPropertyOf 		= 		schema.filter(lambda t: t[1] == rules.value["RDFS_SUBPROPERTY_OF"])\
		  				  		  	  .map(lambda t: (t[0], t[2]))\
		  				  		  	  .collect()
	subClassOf			= 		schema.filter(lambda t: t[1] == rules.value["RDFS_SUBCLASS_OF"])\
	  				   			  	  .map(lambda t: (t[0], t[2]))\
		  			   			  	  .collect()
	equivalentClass		= 		schema.filter(lambda t: t[1] == rules.value["OWL_EQUIVALENT_CLASS"])\
					   			  	  .map(lambda t: (t[0], t[2]))\
					   			  	  .collect()
	equivalentProperty	= 		schema.filter(lambda t: t[1] == rules.value["OWL_EQUIVALENT_PROPERTY"])\
					   		   	  	  .map(lambda t: (t[0], t[2]))\
					   		   	      .collect()
	someValuesFrom		= 	toMap(\
								schema.filter(lambda t: t[1] == rules.value["OWL_SOME_VALUES_FROM"])\
					   		  		  .map(lambda t: (t[0], t[2]))\
					   		  		  .collect()\
								)
	allValuesFrom 		= 	toMap(\
								schema.filter(lambda t: t[1] == rules.value["OWL_ALL_VALUES_FROM"])\
					   		  		  .map(lambda t: (t[0], t[2]))\
					   		  		  .collect()\
								)
	hasValue 			= 	toMap(\
								schema.filter(lambda t: t[1] == rules.value["OWL_HAS_VALUE"])\
					   	  			  .map(lambda t: (t[0], t[2]))\
					   	  			  .collect()\
								)
	#1. intersection
	intersectionList_t1	= \
            toMap(\
            schema.filter(lambda t: t[1] == rules.value["OWL_INTERSECTIONOF"])\
                  .map(lambda t: (t[0], t[2]))\
                  .collect()\
            )\

	intersectionL = \
            schema.filter(lambda t: t[1] == rules.value["OWL_EQUIVALENT_CLASS"] and t[2] in intersectionList_t1)\
                  .map(lambda t: (t[0], intersectionList_t1[t[2]]))\
                  .collect()\

	intersection_t3 = {}
	for iter in intersectionL:
		tmp = iter[1]
		while(True):
			intersection_t3.setdefault(iter[0], []).append(restri_first.value[tmp])
			if(restri_rest.value[tmp] == rules.value["OWL_NIL"]):
				break
			else:
				tmp = restri_rest.value[tmp]
	for iter in intersection_t3:
		for iter2 in intersection_t3[iter]:
			subClassOf.append((iter, iter2))

	#1. union
	unionList = schema.filter(lambda t: t[1] == rules.value["OWL_UNIONOF"])\
					  .map(lambda t: (t[0], t[2]))\
					  .collect()
	for iter in unionList:
		tmp = iter[1]
		while(True):
			subClassOf.append((restri_first.value[tmp], iter[0]))
			if(restri_rest.value[tmp] == rules.value["OWL_NIL"]):
				break
			else:
				tmp = restri_rest.value[tmp]

   	#2 equivalentClass + equivalentProperty
	for iter in equivalentClass:
		subClassOf.append((iter[0], iter[1]))
		subClassOf.append((iter[1], iter[0]))
	for iter in equivalentProperty:
		subClassOf.append((iter[0], iter[1]))
		subClassOf.append((iter[1], iter[0]))

	#3 subClass & subClass => subClass
	#  subProperty & subProperty => subProperty

	#4 someValuesFrom + allValuesFrom + hasValue
	subClassOf_2 = []
	for iter in onProp:
		for iter2 in onProp:
			if iter == iter2:
				continue
			if(onProp[iter] == onProp[iter2]):
				if((iter in someValuesFrom and iter2 in someValuesFrom and (someValuesFrom[iter], someValuesFrom[iter2]) in subClassOf) or (iter in allValuesFrom and iter2 in allValuesFrom and (allValuesFrom[iter], allValuesFrom[iter2]) in subClassOf)):
					subClassOf_2.append((iter, iter2))
			else:
				if(((onProp[iter], onProp[iter2]) in subClassOf) and (hasValue[iter] == hasValue[iter2] or someValuesFrom[iter] == someValuesFrom[iter2] or allValuesFrom[iter] == allValuesFrom[iter2])):
					subClassOf_2.append((iter, iter2))

	subClassOf		=	sc.parallelize(subClassOf)\
				   		  .distinct(parallism)\
						  .persist(storagelevel)
	subPropertyOf	=	sc.parallelize(subPropertyOf)\
						  .distinct(parallism)\
						  .persist(storagelevel)
	
	#5 domain + range
	domains	= schema.filter(lambda t: t[1] == rules.value["RDFS_DOMAIN"])\
					.map(lambda t: (t[0], t[2]))
	ranges	= schema.filter(lambda t: t[1] == rules.value["RDFS_RANGE"])\
				   	.map(lambda t: (t[0], t[2]))

	r5_out1	= domains.map(lambda t: (t[1], t[0])).join(subClassOf)\
					 .map(lambda t: (t[1][0], t[1][1]))
	domains	= domains.union(r5_out1)

	r5_out2	= domains.join(subPropertyOf.map(lambda t: (t[1], t[0])))\
					 .map(lambda t: (t[1][1], t[1][0]))
	domains	= domains.union(r5_out2)

	r5_out3 = ranges.map(lambda t: (t[1], t[0])).join(subClassOf)\
					.map(lambda t: (t[1][0], t[1][1]))
	ranges	= ranges.union(r5_out1)

	r5_out4	= ranges.join(subPropertyOf.map(lambda t: (t[1], t[0])))\
					.map(lambda t: (t[1][1], t[1][0]))
	ranges	= ranges.union(r5_out2)

	#6 subClassOf & subClassOf => equivalentClass
	#  subPropertyOf & subPropertyOf => equivalentProperty
	subClassOf_tmp = set(subClassOf.collect())
	for iter in subClassOf_tmp:
		if((iter[1], iter[0]) in subClassOf_tmp):
			equivalentClass.append((iter[0], iter[1]))
	
	subPropertyOf_tmp = set(subPropertyOf.collect())
	for iter in subPropertyOf_tmp:
		if((iter[1], iter[0]) in subPropertyOf_tmp):
			equivalentProperty.append((iter[0], iter[1]))

	res = schema.union(domains.map(lambda t: (t[0],rules.value["RDFS_DOMAIN"],t[1])))\
				.union(ranges.map(lambda t: (t[0],rules.value["RDFS_RANGE"],t[1])))\
			 	.union(subClassOf.map(lambda t: (t[0],rules.value["RDFS_SUBCLASS_OF"],t[1])))\
			 	.union(subPropertyOf.map(lambda t: (t[0],rules.value["RDFS_SUBPROPERTY_OF"],t[1])))\
			 	.union(sc.parallelize(equivalentProperty).map(lambda t: (t[0],rules.value["OWL_EQUIVALENT_PROPERTY"],t[1])))\
			 	.union(sc.parallelize(equivalentClass).map(lambda t: (t[0],rules.value["OWL_EQUIVALENT_CLASS"],t[1])))\
			 	.distinct(parallism)\
			 	.map(lambda t: "\t".join(t))
	res.saveAsTextFile(outputPath)

if __name__ == "__main__":
	schemaFile = sys.argv[1]
	outputPath = sys.argv[2]
	ruleFile = sys.argv[3]
	startTime = datetime.datetime.now()
	print "startTime: {}".format(startTime)
	main(schemaFile, outputPath, ruleFile)
	endTime = datetime.datetime.now()
	print "\nendTime: {}".format(endTime)
	print "\ntotal time: {}s".format((endTime - startTime).seconds)
