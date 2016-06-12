import sys
import re
import datetime
import random
import itertools
import hashlib
from pyspark import SparkContext, SparkConf, StorageLevel

class Reasoner:

	def __init__(self, schemaFile, instanceFile, outputPath, ruleFile, parallism):
		self.schemaFile = schemaFile
		self.instanceFile = instanceFile
		self.outputPath = outputPath
		self.ruleFile = ruleFile
		#self.DEBUG = True
		self.DEBUG = False
		self.resPrune = True
		#self.resPrune = False
		self.parallism = int(parallism)
		self.storagelevel = StorageLevel.MEMORY_ONLY

	def __toMap(self, list_t):
		return dict((i[0], i[1]) for i in list_t)

#	def __getSmallList(self, bigList, tr, tp):
#		res = []
#		return res
#
#	def __bigToSmall(self, t, tr, tp):
#
#	def __judgeIsNo(self, t, target, tr, tp):
#		res = False
#		if(t[1] == "rdf:type"):
#			tmp = tp.filter(lambda t: (t[2], target) in disjointClassList)
#			if(tmp.count() != 0):
#				res = True
#		else:
#			tmp = tr.filter(lambda t: (t[1], target) in disjointPropertyList)
#			if(tmp.count() != 0):
#				res = True
#		return res
	
	def __prune_sameAsProperty_fun(self, t):
		if(t[1] not in self.sameAsPropertyCenter):
			return t
		else:
			return (t[0], self.sameAsPropertyCenter[t[1]], t[2])
	
	def __prune_equivalentClass_fun(self, t):
		if(t[1] not in self.equivalentClassCenter):
			return t
		else:
			return (t[0], self.equivalentClassCenter[t[1]])
	
	def __prune_equivalentProperty_fun(self, t):
		if(t[1] not in self.equivalentPropertyCenter):
			return t
		else:
			return (t[0], self.equivalentPropertyCenter[t[1]], t[2])
	
	
	def __calcCenter(self, list_data):
		m = {}
		mflag = {}
		res1 = []
		for iter in list_data:
			m.setdefault(iter[0], []).append(iter[1])
			mflag[iter[0]] = 0
		for iter in m:
			if(mflag[iter]):
				continue
			pSet = set()
			queue = []
			queue.append(iter)
			while(len(queue)):
				f = queue[0]
				queue.pop(0)
				if(mflag[f] == 0):
					pSet.add(f)
					queue += m[f]
					mflag[f] = 1
			pSet = list(pSet)
			res1.append((pSet[0], pSet))
		res = {}
		for iter in res1:
			for iter2 in iter[1]:
				res[iter2] = iter[0]
		return res
	
	def __hasKeyFilterFun(self, t):
		tset = set()
		for iter in t[1]:
			tset.add(iter[0])
		for iter in self.hasKey[t[0][1]]:
			if(iter not in tset):
				return False
		return True
	
	def __hasKeyMapFun(self, t):
		tset = set()
		for iter in t[1]:
			tset.add(iter[1])
		tset.add(t[0][1])
		return tuple(tset, t[0][0])
	
	def __sameAsFun(self, t):
		tmp = t[1] + [t[0]]
		minV = min(tmp)
		res = []
		for iter in tmp:
			res.append((minV, iter))
		return res
	
	def __sameAsTable(self, sames):
		res = sames
		lastNum = 0
#		newNum = res.count()
#		if(newNum == 0):
		if(res.isEmpty()):
			return res
		while(True):
			tmp1 = res.union(res.map(lambda t: (t[1], t[0])))
			tmp2 = tmp1.combineByKey(lambda t: [t], lambda t1, t2 : t1+[t2], lambda t1, t2 : t1+t2, self.parallism)\
					   .flatMap(lambda t: self.__sameAsFun(t))\
					   .filter(lambda t: t[0] != t[1])
			res = tmp2.distinct(self.parallism).persist(self.storagelevel)
			lastNum = newNum
			newNum = res.count()
			if(lastNum == newNum):
				break
		return res
	
	def __transitive(self, rdd_t):
		p = rdd_t
		q = p
		l = q.map(lambda t: ((t[1], t[2]), t[0])).partitionBy(self.parallism)
		while True:
			r = q.map(lambda t: ((t[1], t[0]), t[2])).partitionBy(self.parallism)
			q1 = l.join(r).map(lambda t: (t[1][0], t[0][0], t[1][1]))
			q = q1.subtract(p, self.parallism).persist(self.storagelevel)
			#newCount = q.count()
			#if(newCount != 0):
			if(not q.isEmpty()):
				l = q.map(lambda t: ((t[1], t[2]), t[0])).partitionBy(self.parallism)
				s = p.map(lambda t: ((t[1], t[0]), t[2])).partitionBy(self.parallism)
				p1 = l.join(s).map(lambda t: (t[1][0], t[0][0], t[1][1])).persist(self.storagelevel)
				p = p.union(p1).union(q)
			else:
				break
		return p.subtract(rdd_t, self.parallism)

	def __symmRule(self, iterator):
		res = set()
		last = list(iterator)
		lastNum = 0
		while(True):
			newSet = set()
			for t in last:
				if(len(t) == 3):
					if(t[1] in self.inverseOf.value):
						newSet.update(set((t[2], i, t[0]) for i in self.inverseOf.value[t[1]]))
					if(t[1] in self.equivalentPro.value):
						newSet.update(set((t[0], i, t[2]) for i in self.equivalentPro.value[t[1]]))
					if(t[1] in self.sameAs.value):
						newSet.update(set((t[0], i, t[2]) for i in self.sameAs.value[t[1]]))
					if(t[1] in self.symmProp.value):
						newSet.add((t[2], t[1], t[0]))
					if((t[1], t[2]) in self.hasValue2.value):
						newSet.add((t[0], self.hasValue2.value[(t[1], t[2])]))
				else:
					if(t[1] in self.equivalentCls.value):
						newSet.update(set((t[0], i) for i in self.equivalentCls.value[t[1]]))
					if(t[1] in self.hasValue3.value):
						tmp = self.hasValue3.value[t[1]]
						newSet.add((t[0], tmp[0], tmp[1]))
			res.update(newSet)
			newNum = len(res)
			if(newNum == lastNum):
				break
			lastNum = newNum
			last = newSet
			break
		return res
	
	def reasoning(self):
		
		print "main"
		#time
		main_startTime = datetime.datetime.now()
		
		conf = SparkConf().setAppName("KGReasoner")
#						  .setMaster("spark://10.2.28.65:7077")\
#						  .set("spark.executor.memory", "20g")
		sc = SparkContext(conf = conf)
	
		schema = sc.parallelize([])
		instance = sc.parallelize([])
		with open(self.schemaFile) as sf:
			for line in sf:
				if(line.strip() != '' and re.match("#", line.strip()) == None):
					schema = schema.union(sc.textFile(line.strip()).map(lambda t: t.split('\t')))
		with open(self.instanceFile) as inf:
			for line in inf:
				if(line.strip() != '' and re.match("#", line.strip()) == None):
					instance = instance.union(sc.textFile(line.strip()).map(lambda t: t.split('\t')))
		#print "test: {}\t{}".format(schema.count(), instance.count())
	
		if(self.DEBUG):
			print "\t instance triplesNum: {}".format(instance.count())
	
		rule = {}
		with open(self.ruleFile) as rulefile:
			for line in rulefile:
				items = line.strip().split(',')
				rule[items[0]] = items[1]
		rules = sc.broadcast(rule)
		
		#process ontology
	
		cls_List = sc.broadcast(schema.filter(lambda t: t[1] == rules.value["RDF_TYPE"]).map(lambda t: t[0]).collect())
	
		restrictionList = sc.broadcast(\
				schema.filter(lambda t: t[2] == rules.value["OWL_RESTRICTION"])\
					  .map(lambda t: t[0])\
					  .collect()\
		)
	
		restri_first = sc.broadcast(\
					schema.filter(lambda t: t[1] == rules.value["OWL_FIRST"])\
						  .map(lambda t: (t[0], t[2]))\
						  .collectAsMap()\
		)
		restri_rest = sc.broadcast(\
					schema.filter(lambda t: t[1] == rules.value["OWL_REST"])\
						  .map(lambda t: (t[0], t[2]))\
						  .collectAsMap()\
		)
	
		#intersection
#		intersectionList_t1 = \
#				self.__toMap(\
#				schema.filter(lambda t: t[1] == rules.value["OWL_INTERSECTIONOF"])\
#					  .map(lambda t: (t[0], t[2]))\
#					  .collect()\
#				)\
#	
#		intersectionL = \
#				schema.filter(lambda t: t[1] == rules.value["OWL_EQUIVALENT_CLASS"] and t[2] in intersectionList_t1)\
#					  .map(lambda t: (t[0], intersectionList_t1[t[2]]))\
#					  .collect()\
		
		intersectionL = \
				schema.filter(lambda t: t[1] == rules.value["OWL_INTERSECTIONOF"])\
					  .map(lambda t: (t[0], t[2]))\
					  .collect()

		intersection_t3 = {}
		intersectionElements_t = set()
		for iter in intersectionL:
			tmp = iter[1]
			while(True):
				intersection_t3.setdefault(iter[0], []).append(restri_first.value[tmp])
				intersectionElements_t.add(restri_first.value[tmp])
				if(restri_rest.value[tmp] == rules.value["OWL_NIL"]):
					break
				else:
					tmp = restri_rest.value[tmp]
		intersectionList = sc.broadcast(intersection_t3)
		intersectionElements = sc.broadcast(intersectionElements_t)

#		intersection_hash_t = {}
#		for i in intersection_t3.iteritems():
#			tmp = i[1]
#			sort(tmp)
#			hash_key = hashlib.md5("".join(tmp)).hexdigest()
#			intersection_hash_t[hash_key] = i[0]
#		intersection_hash = sc.broadcast(intersection_hash_t)
	
		#propertyChainAxiom
		propertyChainList_t1 = schema.filter(lambda t: t[1] == rules.value["OWL_PROPERTYCHAIN"]).map(lambda t: (t[0], t[2])).collect()
		propertyChainList_t2 = {}
		propertyChainElements_t = set()
		for iter in propertyChainList_t1:
			tmp = iter[1]
			while(True):
				propertyChainList_t2.setdefault(iter[0], []).append(restri_first.value[tmp])
				propertyChainElements_t.add(restri_first.value[tmp])
				if(restri_rest.value[tmp] == rules.value["OWL_NIL"]):
					break
				else:
					tmp = restri_rest.value[tmp]
		propertyChainList = sc.broadcast(propertyChainList_t2)
		propertyChainElements = sc.broadcast(propertyChainElements_t)
	
		#hasKey
		hasKey_t1 = schema.filter(lambda t: t[1] == rules.value["OWL_HASKEY"]).map(lambda t: (t[0], t[2])).collect()
		hasKey_t2 = {}
		propOfHasKey_t = set()
		for iter in hasKey_t1:
			tmp = iter[1]
			while(True):
				hasKey_t2.setdefault(iter[0], []).append(restri_first.value[tmp])
				propOfHasKey_t.add(restri_first.value[tmp])
				if(restri_rest.value[tmp] == rules.value["OWL_NIL"]):
					break
				else:
					tmp = restri_rest.value[tmp]
		self.hasKey = hasKey_t2
		propOfHasKey = sc.broadcast(propOfHasKey_t)
	
		#oneOf
		oneOfList_t = schema.filter(lambda t: t[1] == rules.value["OWL_ONEOF"]).map(lambda t: (t[0], t[2])).collectAsMap()
		oneOfList_t2 = {}
		for iter in oneOfList_t:
			tmp = oneOfList_t[iter]
			while(True):
				oneOfList_t2.setdefault(iter, []).append(restri_first.value[tmp])
				if(restri_rest.value[tmp] == rules.value["OWL_NIL"]):
					break
				else:
					tmp = restri_rest.value[tmp]
		oneOfList = sc.broadcast(oneOfList_t2)
	
		#subClassOf & subProperty & domain & range
		subprop_t = schema.filter(lambda t: t[1] == rules.value["RDFS_SUBPROPERTY_OF"])\
						  .map(lambda t: (t[0], t[2]))\
						  .combineByKey(lambda t: [t], lambda t1, t2 : t1+[t2], lambda t1, t2 : t1+t2, self.parallism)\
						  .collectAsMap()
		subcls_t = schema.filter(lambda t: t[1] == rules.value["RDFS_SUBCLASS_OF"] and t[2] not in restrictionList.value)\
						 .map(lambda t: (t[0], t[2]))\
						 .combineByKey(lambda t: [t], lambda t1, t2 : t1+[t2], lambda t1, t2 : t1+t2, self.parallism)\
						 .collectAsMap()
	
		unionList = schema.filter(lambda t: t[1] == rules.value["OWL_UNIONOF"])\
						  .map(lambda t: (t[0], t[2]))\
						  .collect()
	
		#union => subClassOf
		for iter in unionList:
			tmp = iter[1]
			while(True):
				subcls_t.setdefault(restri_first.value[tmp], []).append(iter[0])
				#subcls_t.add((restri_first.value[tmp], iter[0]))
				if(restri_rest.value[tmp] == rules.value["OWL_NIL"]):
					break
				else:
					tmp = restri_rest.value[tmp]
	
		domain_t = schema.filter(lambda t: t[1] == rules.value["RDFS_DOMAIN"])\
						 .map(lambda t: (t[0], t[2]))\
						 .combineByKey(lambda t: [t], lambda t1, t2 : t1+[t2], lambda t1, t2 : t1+t2, self.parallism)\
						 .collectAsMap()
		range_t = schema.filter(lambda t: t[1] == rules.value["RDFS_RANGE"])\
						.map(lambda t: (t[0], t[2]))\
						.combineByKey(lambda t: [t], lambda t1, t2 : t1+[t2], lambda t1, t2 : t1+t2, self.parallism)\
						.collectAsMap()
	
		subProp = sc.broadcast(subprop_t)
		subCls = sc.broadcast(subcls_t)
		domains = sc.broadcast(domain_t)
		ranges = sc.broadcast(range_t)
	
		#Property
		funcProp = sc.broadcast(\
				set(schema.filter(lambda t: t[2] == rules.value["OWL_FUNCTIONAL_PROPERTY"])\
					  .map(lambda t: t[0])\
					  .collect())\
		)
		inverFunProp = sc.broadcast(\
				set(schema.filter(lambda t: t[2] == rules.value["OWL_INVERSE_FUNCTIONAL_PROPERTY"])\
					  .map(lambda t: t[0])\
					  .collect())\
		)
		tranProp = sc.broadcast(\
				set(schema.filter(lambda t: t[2] == rules.value["OWL_TRANSITIVE_PROPERTY"])\
					  .map(lambda t: t[0])\
					  .collect())\
		)
		self.symmProp = sc.broadcast(\
				set(schema.filter(lambda t: t[2] == rules.value["OWL_SYMMETRIC_PROPERTY"])\
					  .map(lambda t: t[0])\
					  .collect())\
		)
	
		#onProperty & someValueFrom & allValueFrom & hasValue
		onProp = sc.broadcast(\
					schema.filter(lambda t: t[1] == rules.value["OWL_ON_PROPERTY"])\
						  .map(lambda t: (t[0], t[2]))\
						  .collectAsMap()\
		)
		hasValue = sc.broadcast(\
					schema.filter(lambda t: t[1] == rules.value["OWL_HAS_VALUE"])\
						  .map(lambda t: (t[0], t[2]))\
						  .collectAsMap()\
		)
		hasValue2_t = {}
		for iter in hasValue.value:
			hasValue2_t[(onProp.value[iter], hasValue.value[iter])] = iter
		self.hasValue2 = sc.broadcast(hasValue2_t)

		hasValue3_t = {}
		for iter in hasValue2_t.iteritems():
			hasValue3_t[iter[1]] = iter[0]
		self.hasValue3 = sc.broadcast(hasValue3_t)

		someValueFromPair = sc.broadcast(\
					schema.filter(lambda t: t[1] == rules.value["OWL_SOME_VALUES_FROM"])\
						  .map(lambda t: ((onProp.value[t[0]], t[2]), t[0]))\
						  .collectAsMap()\
		)
		propOfSomeValueFrom_t = set()
		classOfSomeValueFrom_t = set()
		for iter in someValueFromPair.value:
			propOfSomeValueFrom_t.add(iter[0])
			classOfSomeValueFrom_t.add(iter[1])
		propOfSomeValueFrom = sc.broadcast(propOfSomeValueFrom_t)
		classOfSomeValueFrom = sc.broadcast(classOfSomeValueFrom_t)
	
		allValueFrom = sc.broadcast(\
					schema.filter(lambda t: t[1] == rules.value["OWL_ALL_VALUES_FROM"])\
						  .map(lambda t: (t[0], t[2]))\
						  .collectAsMap()\
		)
		propOfAllValueFrom_t = set()
		for iter in allValueFrom.value:
			propOfAllValueFrom_t.add(onProp.value[iter])
		propOfAllValueFrom = sc.broadcast(propOfAllValueFrom_t)
	
		#inverseOf
		inverseOf_t1 = schema.filter(lambda t: t[1] == rules.value["OWL_INVERSE_OF"])\
							 .map(lambda t: (t[0], t[2]))
		inverseOf_t2 = inverseOf_t1.map(lambda t: (t[1], t[0]))
		self.inverseOf = sc.broadcast(\
					inverseOf_t1.union(inverseOf_t2)\
								.combineByKey(lambda t: [t], lambda t1, t2 : t1+[t2], lambda t1, t2 : t1+t2, self.parallism)\
								.collectAsMap()\
		)
	
		#equivalentClass & equivalentProperty
		equivalentCls_t1 = schema.filter(lambda t: t[1] == rules.value["OWL_EQUIVALENT_CLASS"])\
							  	 .map(lambda t: (t[0], t[2]))
		equivalentCls_t2 = equivalentCls_t1.map(lambda t: (t[1], t[0]))
		self.equivalentCls = sc.broadcast(\
					equivalentCls_t1.union(equivalentCls_t2)\
									.combineByKey(lambda t: [t], lambda t1, t2 : t1+[t2], lambda t1, t2 : t1+t2, self.parallism)\
									.collectAsMap()\
		)
		self.equivalentClassCenter = self.__calcCenter(equivalentCls_t1.union(equivalentCls_t2).collect())
	
		equivalentPro_t1 = schema.filter(lambda t: t[1] == rules.value["OWL_EQUIVALENT_PROPERTY"])\
								 .map(lambda t: (t[0], t[2]))
		equivalentPro_t2 = equivalentPro_t1.map(lambda t: (t[1], t[0]))
		self.equivalentPro = sc.broadcast(\
					equivalentPro_t1.union(equivalentPro_t2)\
									.combineByKey(lambda t: [t], lambda t1, t2 : t1+[t2], lambda t1, t2 : t1+t2, self.parallism)\
									.collectAsMap()\
		)
		self.equivalentPropertyCenter = self.__calcCenter(equivalentPro_t1.union(equivalentPro_t2).collect())
		
		#sameAs
		sameAsProperty = schema.filter(lambda t: t[1] == rules.value["OWL_SAME_AS"])\
			  				   .map(lambda t: (t[0], t[2]))\
			  				   .collectAsMap()
		self.sameAsPropertyCenter = self.__calcCenter(sameAsProperty)
		self.sameAs = sc.broadcast(sameAsProperty)

		onClass = sc.broadcast(\
					schema.filter(lambda t: t[1] == rules.value["OWL_ON_CLASS"])\
						  .map(lambda t: (t[0], t[2]))\
						  .collectAsMap()\
		)
		#maxCardinality
		maxCardinality = sc.broadcast(\
					schema.filter(lambda t: t[1] == rules.value["OWL_MAXCARDINALITY"])\
						  .map(lambda t: (t[0], 1))\
						  .collectAsMap()\
		)
		propOfMaxCardinality = sc.broadcast(set(onProp.value[i] for i in maxCardinality.value))

		maxQualifiedCardinality = sc.broadcast(\
					schema.filter(lambda t: t[1] == rules.value["OWL_MAXQUALIFIEDCARDINALITY"])\
						  .map(lambda t: (t[0], 1))\
						  .collectAsMap()\
		)
		mqc_t1 = set()
		for i in maxQualifiedCardinality.value:
			if(i in onProp.value):
				mqc_t1.add(onProp.value[i])
		propOfMaxQualifiedCardinality = sc.broadcast(mqc_t1)
		#propOfMaxQualifiedCardinality = sc.broadcast(set(onProp.value[i] for i in maxQualifiedCardinality.value))

		mqc_t2 = set()
		for i in maxQualifiedCardinality.value:
			if(i in onClass.value):
				mqc_t2.add(onClass.value[i])
		#clsOfMaxQualifiedCardinality = sc.broadcast(set(onClass.value[i] for i in maxQualifiedCardinality.value))
		clsOfMaxQualifiedCardinality = sc.broadcast(mqc_t2)

	
	
		#process instance triple
		tr = instance.filter(lambda t: t[1] != rules.value["OWL_SAME_AS"] and t[1] != rules.value["RDF_TYPE"])\
					 .map(lambda t: (t[0], t[1], t[2]))
	
		tp = instance.filter(lambda t: t[1] == rules.value["RDF_TYPE"])\
					 .map(lambda t: (t[0], t[2]))
	
		sames = instance.filter(lambda t: t[1] == rules.value["OWL_SAME_AS"])\
						.map(lambda t: (t[0], t[2]))
#		tr.persist(self.storagelevel)
#		tp.persist(self.storagelevel)
#		sames.persist(self.storagelevel)
	
		tr_bak = tr
		tp_bak = tp
		sm_bak = sames
	
		#c, owl:oneOf, x LIST[?x, ?y1, ..., ?yn] => yi, rdf:type, ?c
		tmp = []
		for iter in oneOfList.value:
			for iter2 in oneOfList.value[iter]:
				tmp.append((iter2, iter))
		tp.union(sc.parallelize(tmp))
	
		#start reasoning process
		flag = True
		tr_flag = True
		tp_flag = True
		isFirst = True
	
		loopNums = 0
	
		lastTr = tr.persist(self.storagelevel)
		lastTp = tp.persist(self.storagelevel)

		loop_startTime = datetime.datetime.now()
		print "\t----------------------------------------------------------"
		print "\tschemaProcessTime: {}s".format((loop_startTime - main_startTime).seconds)
		print "\t----------------------------------------------------------"
	
		while flag:
			loopNums += 1
			#time
			print "\nloop{}: ".format(loopNums)
			startTime = datetime.datetime.now()
			print "----------------------------------------------------------"
			print "\tstartTime: {}".format(startTime)
			print "----------------------------------------------------------"
	
			perTp = sc.parallelize([]).persist(self.storagelevel)
			perTr = sc.parallelize([]).persist(self.storagelevel)

			trlist = []
			tplist = []

			#RDFS rule 7
			#s p o & p rdfs:subPropertyOf q => s q o
			r7_t = lastTr.filter(lambda t: t[1] in subProp.value)\
					 .map(lambda t: ((t[0], t[2]), subProp.value[t[1]]))\
					 .flatMapValues(list)\
					 .map(lambda t: (t[0][0], t[1], t[0][1]))\
					 .persist(self.storagelevel)
			trlist.append(r7_t)

			if(self.DEBUG):
				print "\tsubPropertyOf: {}".format(r7_t.count())

			r23_in = r7_t.union(lastTr)
			#RDFS rule 2 and 3
			#s p o & p rdfs:domain x => s rdf:type x
			#s p o & p rdfs:range  x => o rdf:type x
			r2_out = r23_in.filter(lambda t: t[1] in domains.value)\
					   .map(lambda t: (t[0], domains.value[t[1]]))\
					   .flatMapValues(list)\
					   .persist(self.storagelevel)
			if(self.DEBUG):
				print "\tdomain + range: {}".format(r2_out.count())
			r3_out = r23_in.filter(lambda t: t[1] in ranges.value)\
					   .map(lambda t: (t[2], ranges.value[t[1]]))\
					   .flatMapValues(list)\
					   .persist(self.storagelevel)
			if(self.DEBUG):
				print "\tdomain + range: {}".format(r3_out.count())
			r23_out = r2_out.union(r3_out)
			tplist.append(r23_out)
			#RDFS rule 9
			#s rdf:type x & x rdfs:subClassOf y => s rdf:type y
			r9_out = r23_out.union(lastTp).filter(lambda t: t[1] in subCls.value)\
					   .map(lambda t: (t[0], subCls.value[t[1]]))\
					   .flatMapValues(list)\
					   .persist(self.storagelevel)
			tplist.append(r9_out)

			if(self.DEBUG):
				print "\tsubClassOf: {}".format(r9_out.count())
		
			#OWL rule 3
			#v p u & p rdf:type owl:SymmetricProperty => u p v
			o3_out = r7_t.union(lastTr).filter(lambda t: t[1] in self.symmProp.value)\
					   .map(lambda t: (t[2], t[1], t[0]))\
					   .persist(self.storagelevel)
			trlist.append(o3_out)

			if(self.DEBUG):
				print "\tSymmetricProperty: {}".format(o3_out.count())

			#OWL rule 8a and 8b
			#p owl:inverseOf q & v p w => w q v
			#p owl:inverseOf q & v q w => w p v
			o8_out = r7_t.union(lastTr).filter(lambda t: t[1] in self.inverseOf.value)\
					   .map(lambda t: ((t[2], t[0]), self.inverseOf.value[t[1]]))\
					   .flatMapValues(list)\
					   .map(lambda t: (t[0][0], t[1], t[0][1]))\
					   .persist(self.storagelevel)
			trlist.append(o8_out)
			if(self.DEBUG):
				print "\tinverseOf: {}".format(o8_out.count())

			#OWL RL rule prp-eqp1/2
			#x p1 y & p1 owl:equivalentProperty p2 => x p2 y
			#x p2 y & p1 owl:equivalentProperty p2 => x p1 y
			rl_prp_eqp_out = r7_t.union(lastTr).filter(lambda t: t[1] in self.equivalentPro.value)\
							   .map(lambda t: ((t[0], t[2]), self.equivalentPro.value[t[1]]))\
							   .flatMapValues(list)\
							   .map(lambda t: (t[0][0], t[1], t[0][1]))\
							   .persist(self.storagelevel)
			trlist.append(rl_prp_eqp_out)

			if(self.DEBUG):
				print "\tequivalentProperty: {}".format(rl_prp_eqp_out.count())

			#OWL RL rule eq-rep-p
			#s p o & p owl:sameAs p' => s p' o
			rl_eq_rep_p_out = r7_t.union(lastTr).filter(lambda t: t[1] in self.sameAs.value)\
									.map(lambda t: (t[0], self.sameAs.value[t[1]], t[2]))
			trlist.append(rl_eq_rep_p_out)

			#OWL RL rule cax-eqc1/2
			#x rdf:type c1 c1 owl:equivalentClass c2 => x rdf:type c2
			#x rdf:type c2 c1 owl:equivalentClass c2 => x rdf:type c1
			rl_cax_eqc_out = r23_out.union(r9_out).union(lastTp).filter(lambda t: t[1] in self.equivalentCls.value)\
							   .map(lambda t: (t[0], self.equivalentCls.value[t[1]]))\
							   .flatMapValues(list)\
							   .persist(self.storagelevel)
			tplist.append(rl_cax_eqc_out)

			if(self.DEBUG):
				print "\tequivalentClass: {}".format(rl_cax_eqc_out.count())

			if(len(self.hasValue2.value) != 0):
				#OWL rule 14a
				#u p w & v owl:hasValue w & v owl:onProperty p => u rdf:type v
				o14a_out = r7_t.union(lastTr).filter(lambda t: (t[1], t[2]) in self.hasValue2.value)\
							 .map(lambda t: (t[0], self.hasValue2.value[(t[1], t[2])]))\
							 .persist(self.storagelevel)
				tplist.append(o14a_out)
	
				if(self.DEBUG):
					print "\thasValue1: {}".format(o14a_out.count())
	
			if(len(onProp.value) != 0 and len(hasValue.value) != 0):
				#OWL rule 14b
				#u rdf:type v & v owl:hasValue w & v owl:onProperty p => u p w
				o14b_out = r23_out.union(r9_out).union(lastTp).filter(lambda t: t[1] in onProp.value and t[1] in hasValue.value)\
							 .map(lambda t: (t[0], onProp.value[t[1]], hasValue.value[t[1]]))\
							 .persist(self.storagelevel)
				trlist.append(o14b_out)
				
				if(self.DEBUG):
					print "\thasValue2: {}".format(o14b_out.count())

			intersect_front = sc.union([lastTp] + tplist).filter(lambda t: t[1] in intersectionList.value)\
									.map(lambda t: (t[0], intersectionList.value[t[1]]))\
									.flatMapValues(lambda t: t)\
									.persist(self.storagelevel)
			tplist.append(intersect_front)

#			#OWL propertyChainAxiom
#			pch_out = sc.parallelize([])
#			tmp_tr = tr.union(perTr).filter(lambda t: t[1] in propertyChainElements.value).persist(self.storagelevel)
#			for iter in propertyChainList.value:
#				iterNum = len(propertyChainList.value[iter]) - 1
#				res = tmp_tr.filter(lambda t: t[1] == propertyChainList.value[iter][0]).map(lambda t: (t[2], t[0]))
#				for i in range(iterNum):
#					tmp = tmp_tr.filter(lambda t: t[1] == propertyChainList.value[iter][i+1]).map(lambda t: (t[0], t[2]))
#					res = res.join(tmp).map(lambda t: (t[1][1], t[1][0]))
#				pch_out = pch_out.union(res.map(lambda t: (t[1], iter, t[0])))
#			perTr = perTr.union(pch_out)
#			if(self.DEBUG):
#				proCh2 = pch_out.count()
#				print "\tpropertyChainAxiom: {}".format(proCh2)
	
			ruleSet1Time = datetime.datetime.now()
			print "----------------------------------------------------------"
			print "\trule1-12'sTime: {}s".format((ruleSet1Time-startTime).seconds)
			print "----------------------------------------------------------"

			#Finally
			perTr = sc.union(trlist).distinct(self.parallism).subtract(tr, self.parallism).persist(self.storagelevel)
			perTp = sc.union(tplist).distinct(self.parallism).subtract(tp, self.parallism).persist(self.storagelevel)

			subtractTime = datetime.datetime.now()
			print "----------------------------------------------------------"
			print "\tsubtractTime: {}s".format((subtractTime-ruleSet1Time).seconds)
			print "----------------------------------------------------------"
	
			if(perTr.isEmpty()):
				tr_flag = False
			else:
				tr_flag = True
			if(perTp.isEmpty()):
				tp_flag = False
			else:
				tp_flag = True
	
			if(self.DEBUG):
				print "\tcount_newType: {}  count_newTriple: {}".format(count_newType, count_newTriple)
	
			if(not isFirst and (not tr_flag) and (not tp_flag)):
				flag = False

			#OWL rule 4
			#p rdf:type owl:TransitiveProperty & u p w & w p v => u p v
			o4_out = sc.parallelize([])
			if(isFirst or tr_flag):
				tran_perTr = perTr.filter(lambda t: t[1] in tranProp.value).persist(self.storagelevel)
#				tran_perTr_Num = tran_perTr.count()
				if(isFirst or (not tran_perTr.isEmpty())):
					o4_t = tr.filter(lambda t: t[1] in tranProp.value)\
							 .union(tran_perTr)\
							 .persist(self.storagelevel)
					o4_out = self.__transitive(o4_t).persist(self.storagelevel)
#					perTr = perTr.union(o4_out)
		
					if(self.DEBUG):
						print "\tTransitiveProperty: {}".format(o4_out.count())
	
			if(isFirst or tr_flag or tp_flag):
				#OWL rule 16
				#v owl:allValuesFrom u, v owl:onProperty p, w rdf:type v, w p x => x rdf:type u
				#v owl:allValuesFrom u & v owl:onProperty p & w rdf:type v => <<w,p>,u>
				o16_t1 = tp.union(perTp)\
						   .filter(lambda t: t[1] in allValueFrom.value)\
						   .map(lambda t: ((t[0], onProp.value[t[1]]), allValueFrom.value[t[1]]))\
						   .partitionBy(self.parallism)\
						   .persist(self.storagelevel)
				#v owl:allValuesFrom u & v owl:onProperty p & w p x => <<w,p>,x>
				o16_t2 = tr.union(perTr).union(o4_out)\
						   .filter(lambda t: t[1] in propOfAllValueFrom.value)\
						   .map(lambda t: ((t[0], t[1]), t[2]))\
						   .partitionBy(self.parallism)\
						   .persist(self.storagelevel)
				o16_out = o16_t1.join(o16_t2).map(lambda t: (t[1][1], t[1][0])).persist(self.storagelevel)
#				perTp = perTp.union(o16_out)
	
				if(self.DEBUG):
					print "\tallValuesFrom: {}".format(o16_out.count())

				#OWL rule 15
				#v owl:someValuesFrom w, v owl:onProperty p, u p x, x rdf:type w => u rdf:type v
				#tmp_tr = tr.union(perTr).distinct(self.parallism).persist(self.storagelevel)
				#tmp_tp = tp.union(perTp).distinct(self.parallism).persist(self.storagelevel)
				o15_t1 = tr.union(perTr).union(o4_out)\
						   .filter(lambda t: t[1] in propOfSomeValueFrom.value)\
						   .map(lambda t: (t[2], (t[1], t[0])))\
						   .partitionBy(self.parallism)\
						   .persist(self.storagelevel)
				o15_t2 = tp.union(perTp).union(o16_out)\
						   .filter(lambda t: t[1] in classOfSomeValueFrom.value)\
						   .partitionBy(self.parallism)\
						   .persist(self.storagelevel)
				o15_out = o15_t1.join(o15_t2)\
								.filter(lambda t: (t[1][0][0], t[1][1]) in someValueFromPair.value)\
								.map(lambda t: (t[1][0][1], someValueFromPair.value[(t[1][0][0], t[1][1])]))
#				perTp = perTp.union(o15_out)
	
				if(self.DEBUG):
					print "\tsomeValuesFrom: {}".format(o15_out.count())
	
				#intersection restriction
				intersect_out = sc.parallelize([])
				tmp_tp = tp.union(perTp).union(o16_out).union(o15_out).filter(lambda t: t[1] in intersectionElements.value).partitionBy(self.parallism).persist(self.storagelevel)
				for iter in intersectionList.value:
					iterNum = len(intersectionList.value[iter])-1
					perRes = tmp_tp.filter(lambda t: t[1] == intersectionList.value[iter][0])
					for i in range(iterNum):
						nextElement = tmp_tp.filter(lambda t: t[1] == intersectionList.value[iter][i+1])
						perRes = perRes.join(nextElement)
					intersect_out = intersect_out.union(perRes.map(lambda t: (t[0], iter)))
#				perTp = perTp.union(intersect_out)
	
				if(self.DEBUG):
					print "\tintersectionOf: {}".format(intersect_out.count())
	
			if(flag):
				perTr = perTr.union(o4_out).distinct(self.parallism)
				perTp = sc.union([perTp, intersect_out, o15_out, o16_out]).distinct(self.parallism)
				tr = tr.union(perTr)#.persist(self.storagelevel)
				tp = tp.union(perTp)#.persist(self.storagelevel)
				lastTr = perTr.persist(self.storagelevel)
				lastTp = perTp.persist(self.storagelevel)
			isFirst = False
	
			#time
			endTime = datetime.datetime.now()
			print "----------------------------------------------------------"
			print "\tendTime: {}".format(endTime)
			print "\n\trule15~17/1,2,4'sTime: {}s".format((endTime-subtractTime).seconds)
			print "\n\ttotalTime: {}s".format((endTime-startTime).seconds)
			print "----------------------------------------------------------"
			
		#end loop
	
		print "\ntotal loopNums = " + str(loopNums)
		endLoopTime = datetime.datetime.now()
		print "----------------------------------------------------------"
		print "\nendLoopTime: {}s".format((endLoopTime-loop_startTime).seconds)
		print "----------------------------------------------------------"
	
		#process sameAs rules
	
		if(len(funcProp.value) != 0):
			#OWL rule 1
			#p rdf:type owl:FunctionalProperty & u p v & u p w => v owl:sameAs w
			o1_t = tr.filter(lambda t: t[1] in funcProp.value)\
					 .map(lambda t: ((t[0], t[1]), t[2]))\
					 .partitionBy(self.parallism)
			o1_out = o1_t.join(o1_t)\
						 .map(lambda t: (t[1][0], t[1][1]))\
						 .filter(lambda t: t[0] != t[1])
			sames = sames.union(o1_out)
		
			if(self.DEBUG):
				print "\tFunctionalProperty: {}".format(o1_out.count())
	
		if(len(inverFunProp.value) != 0):
			#OWL rule 2
			#p rdf:type owl:InverseFunctionalProperty & v p u & w p u => v owl:sameAs w
			o2_t = tr.filter(lambda t: t[1] in inverFunProp.value)\
					 .map(lambda t: ((t[1], t[2]), t[0]))\
					 .partitionBy(self.parallism)
			o2_out = o2_t.join(o2_t)\
						 .map(lambda t: (t[1][0], t[1][1]))\
						 .filter(lambda t: t[0] != t[1])
			sames = sames.union(o2_out)
		
			if(self.DEBUG):
				print "\tInverseFunctionalProperty: {}".format(o2_out.count())
	
		if(len(maxCardinality.value) != 0):
			#OWL RL rule cls-maxc2
			#u rdf:type x & x owl:onProperty p & u p y1 & u p y2 & x owl:maxCardinality 1 => y1 owl:sameAs y2
			rl_cls_maxc2_t1 = tp.filter(lambda t: t[1] in maxCardinality.value and int(maxCardinality.value[t[1]]) == 1)\
							   .map(lambda t: ((t[0], onProp.value[t[1]]), ""))\
							   .partitionBy(self.parallism)\
							   .persist(self.storagelevel)
			rl_cls_maxc2_t2 = tr.filter(lambda t: t[1] in propOfMaxCardinality.value)\
								.map(lambda t: ((t[0], t[1]), t[2]))\
								.partitionBy(self.parallism)\
								.persist(self.storagelevel)
			rl_cls_maxc2_t = rl_cls_maxc2_t1.join(rl_cls_maxc2_t2)\
							   				.map(lambda t: (t[0], t[1][1]))\
							   				.persist(self.storagelevel)
			rl_cls_maxc2_out = rl_cls_maxc2_t.join(rl_cls_maxc2_t)\
											 .map(lambda t: (t[1][0], t[1][1]))\
											 .filter(lambda t: t[0] != t[1])\
											 .persist(self.storagelevel)
			if(self.DEBUG):
				print "\tmaxCardinality: {}".format(rl_cls_maxc2_out.count())
			sames = sames.union(rl_cls_maxc2_out)
	
		if(len(maxQualifiedCardinality.value) != 0):
			#x owl:maxQualifiedCardinality 1 & x owl:onProperty p & x owl:onClass c & u rdf:type x & u p y1 & y1 rdf:type c & u p y2 & y2 rdf:type c => y1 owl:sameAs y2

			for iter in maxQualifiedCardinality.value.iteritems():
				print iter[0] + "\t" + str(iter[1])

		#	test = tp.filter(lambda t: t[1] in maxQualifiedCardinality.value).count()
		#	print "TEST test: {}".format(test)

			rl_cls_maxqc3_t1 = tp.filter(lambda t: t[1] in maxQualifiedCardinality.value and int(maxQualifiedCardinality.value[t[1]]) == 1 and onClass.value[t[1]] != rules.value["OWL_THING"])\
								.map(lambda t: ((t[0], onProp.value[t[1]], onClass.value[t[1]]), ""))\
								.partitionBy(self.parallism)\
								.persist(self.storagelevel)

		#	print "TEST rl_cls_maxqc3_t1: {}".format(rl_cls_maxqc3_t1.count())

			rl_cls_maxqc3_t2 = tr.filter(lambda t: t[1] in propOfMaxQualifiedCardinality.value)\
								 .map(lambda t: (t[2], (t[0], t[1])))\
								 .partitionBy(self.parallism)\
								 .join(tp.filter(lambda t: t[1] in clsOfMaxQualifiedCardinality.value).partitionBy(self.parallism))\
								 .map(lambda t: ((t[1][0][0], t[1][0][1], t[1][1]), t[0]))\
								 .join(rl_cls_maxqc3_t1)\
								 .map(lambda t: (t[0], t[1][0]))\
								 .persist(self.storagelevel)

		#	print "TEST rl_cls_maxqc3_t2: {}".format(rl_cls_maxqc3_t2.count())

			rl_cls_maxqc3_out = rl_cls_maxqc3_t2.join(rl_cls_maxqc3_t2)\
												.map(lambda t: (t[1][0], t[1][1]))\
												.filter(lambda t: t[0] != t[1])\
												.persist(self.storagelevel)
			if(self.DEBUG):
				print "\tmaxQualifiedCardinality_0: {}".format(rl_cls_maxqc3_out.count())
		
			#u rdf:type x & x owl:onProperty p & u p y1 & u p y2 & x owl:maxQualifiedCardinality 1 & x owl:onClass owl:Thing => y1 owl:sameAs y2
			rl_cls_maxqc4_t = tp.filter(lambda t: t[1] in maxQualifiedCardinality.value and int(maxQualifiedCardinality.value[t[1]]) == 1 and onClass.value[t[1]] == rules.value["OWL_THING"])\
							 	.map(lambda t: ((t[0], onProp.value[t[1]]), ""))\
								.partitionBy(self.parallism)\
								.join(tr.filter(lambda t: t[1] in propOfMaxQualifiedCardinality.value).map(lambda t: ((t[0], t[1]), t[2])).partitionBy(self.parallism))\
							 	.map(lambda t: (t[0], t[1][1]))\
							 	.persist(self.storagelevel)
			rl_cls_maxqc4_out = rl_cls_maxqc4_t.join(rl_cls_maxqc4_t)\
											 .map(lambda t: (t[1][0], t[1][1]))\
											 .filter(lambda t: t[0] != t[1])\
											 .persist(self.storagelevel)
			if(self.DEBUG):
				print "\tmaxQualifiedCardinality_1: {}".format(rl_cls_maxqc4_out.count())
		
			sames = sames.union(rl_cls_maxqc3_out)\
					 	 .union(rl_cls_maxqc4_out)
		
		if(len(self.hasKey) != 0):
			#c owl:hasKey u & List[u, p1, ..., pn] & x rdf:type c & y rdf:type c & x pi zi & y pi zi => x owl:sameAs y
			hasKey_t1 = tp.filter(lambda t: t[1] in self.hasKey)\
						  .partitionBy(self.parallism)\
						  .join(tr.filter(lambda t: t[1] in propOfHasKey.value).map(lambda t: (t[0], (t[1], t[2]))).partitionBy(self.parallism))\
						  .map(lambda t: ((t[0], t[1][0]), t[1][1]))\
						  .combineByKey(lambda t: [t], lambda t1, t2 : t1+[t2], lambda t1, t2 : t1+t2, self.parallism)\
						  .filter(self.__hasKeyFilterFun)\
						  .map(self.__hasKeyMapFun)\
						  .partitionBy(self.parallism)\
						  .persist(self.storagelevel)
			hasKey_out = hasKey_t1.join(hasKey_t1)\
								  .map(lambda t: (t[1][0], t[1][1]))\
								  .persist(self.storagelevel)
			if(self.DEBUG):
				print "\thasKey: {}".format(hasKey_out.count())

			sames = sames.union(hasKey_out)
	
		#self.__sameAsTable
		#OWL rule 7
		#v owl:sameAs w & w owl:sameAs u => v owl:sameAs u

		if(self.DEBUG):
			print "sames1: {}".format(sames.count())

		sames = self.__sameAsTable(sames).map(lambda t: (t[1], t[0])).partitionBy(self.parallism).distinct(self.parallism)

		if(self.DEBUG):
			print "sames2: {}".format(sames.count())

		sameAsTime = datetime.datetime.now()
		print "----------------------------------------------------------"
		print "\nendSameAsTime: {}s".format((sameAsTime-endLoopTime).seconds)
		print "----------------------------------------------------------"
		
#		#OWL RL rule eq-rep-s
#		#s p o & s owl:sameAs s' => s' p o
#		tr = tr.union(tr.map(lambda t: (t[0], (t[1], t[2])))\
#			   .join(sames)\
#			   .map(lambda t: (t[1][1], t[1][0][0], t[1][0][1])))
#	
#		#OWL RL rule eq-rep-o
#		#s p o & o owl:sameAs o' => s p o'
#		tr = tr.union(tr.map(lambda t: (t[2], (t[0], t[1])))\
#			   .join(sames)\
#			   .map(lambda t: (t[1][0][0], t[1][0][1], t[1][1]))\
#			   .distinct(self.parallism))
	
	
		if(self.DEBUG):
			trNum_prune_front = tr.count()
			tpNum_prune_front = tp.count()

		if(self.resPrune):
			#sameAs individual and schema
			#samesNum = sames.count()
			#if(samesNum != 0):
			if(not sames.isEmpty()):
				prune1_t1 = tr.map(lambda t: (t[0], (t[1], t[2]))).partitionBy(self.parallism).join(sames).persist(self.storagelevel)
				prune1_out_left = prune1_t1.map(lambda t: (t[1][1], t[1][0][0], t[1][0][1]))
				prune1_out_oldleft = prune1_t1.map(lambda t: (t[0], t[1][0][0], t[1][0][1]))
				tr = prune1_out_left.union(tr.subtract(prune1_out_oldleft, self.parallism))
		
				prune1_t2 = tr.map(lambda t: (t[2], (t[0], t[1]))).partitionBy(self.parallism).join(sames).persist(self.storagelevel)
				prune1_out_right = prune1_t2.map(lambda t: (t[1][0][0], t[1][0][1], t[1][1]))
				prune1_out_oldright = prune1_t2.map(lambda t: (t[1][0][0], t[1][0][1], t[0]))
				tr = prune1_out_right.union(tr.subtract(prune1_out_oldright, self.parallism))
		
				prune1_t3 = tp.partitionBy(self.parallism).join(sames).persist(self.storagelevel)
				prune1_out_tp = prune1_t3.map(lambda t: (t[1][1], t[1][0]))
				prune1_out_oldtp = prune1_t3.map(lambda t: (t[0], t[1][0]))
				tp = prune1_out_tp.union(tp.subtract(prune1_out_oldtp, self.parallism))
	
			if(len(self.sameAsPropertyCenter) != 0):
				tr = tr.map(self.__prune_sameAsProperty_fun)
	
			#equivalentClass + equivalentProperty
			if(len(self.equivalentClassCenter) != 0):
				tp = tp.map(self.__prune_equivalentClass_fun).distinct(self.parallism)

			if(len(self.equivalentPropertyCenter) != 0):
				tr = tr.map(self.__prune_equivalentProperty_fun).distinct(self.parallism)
	
			#Anonymous
#			tp = tp.filter(lambda t: re.match("_:", t[1]) == None)
	

		if(True or self.DEBUG):
			trNum_prune_final = tr.count()
			tpNum_prune_final = tp.count()
			#print "\nfinally new tirples: {}".format(tr.count() + tp.count() + sames.count())

			if(self.DEBUG):
				print "\nprune tr:{} tp:{}".format(trNum_prune_front - trNum_prune_final, tpNum_prune_front - tpNum_prune_final)

			print "\nfinally new tirples: tr:{}, tp:{}, sameAs:{}".format(trNum_prune_final, tpNum_prune_final, sames.count())

		pruneTime = datetime.datetime.now()
		print "----------------------------------------------------------"
		print "\npruneTime: {}s".format((pruneTime-sameAsTime).seconds)
		print "----------------------------------------------------------"

		#calc new triples
		tr = tr.subtract(tr_bak, self.parallism).persist(self.storagelevel)
		#tp = tp.filter(lambda t: re.match("_:", t[1]) == None).subtract(tp_bak, self.parallism).persist(self.storagelevel)
		tp = tp.subtract(tp_bak, self.parallism).persist(self.storagelevel)
		sames = sames.subtract(sm_bak, self.parallism).persist(self.storagelevel)
	
		#Finish and save result
		tr.union(tp.map(lambda t: (t[0], rules.value["RDF_TYPE"], t[1])))\
		  .union(sames.map(lambda t: (t[0], rules.value["OWL_SAME_AS"], t[1])))\
		  .map(lambda t: '\t'.join(t))\
		  .saveAsTextFile(self.outputPath)

		saveTime = datetime.datetime.now()
		print "----------------------------------------------------------"
		print "\nsaveFileTime: {}s".format((saveTime-pruneTime).seconds)
		print "----------------------------------------------------------"

if __name__ == "__main__":
	#argv
	schemaFile = sys.argv[1]
	instanceFile = sys.argv[2]
	outputPath = sys.argv[3]
	ruleFile = sys.argv[4]
	parallism = sys.argv[5]
	print schemaFile + " " + instanceFile + " " + outputPath + "\n"
	#start time
	startTime = datetime.datetime.now()
	print "startTime: {}".format(startTime)

	r = Reasoner(schemaFile, instanceFile, outputPath, ruleFile, parallism)
	r.reasoning()

	endTime = datetime.datetime.now()
	print "\nendTime: {}".format(endTime)
	print "\ntotal time: {}s".format((endTime - startTime).seconds)
