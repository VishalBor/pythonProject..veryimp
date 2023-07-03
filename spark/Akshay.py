
# Name="vishal"
# print(Name[::-1])
#
#
# list = ["HELLO","WORLD","PYTHON"]
#

# list1=[1,0,1,0,1,0,1,1]
# list2=[]
# for i in list1:
#     if i ==0:
#         list2.append(i)
# for j in list1:
#     if j==1:
#         list2.append(j)
# print(list2)


# list1 = [1, 2, 3]
# list2 = [4, 5, 6]
# output=[x + y for x , y in zip(list1,list2)]
# print(output)

# list1 = [1, 2, 3]
# list2 = [4, 5, 6]
# ooutput={x:y for x,y in zip(list1,list2)}
# print(ooutput)


# list1 = [1, 2, 3]
# list2 = [4, 5, 6]

# output = {}
#
# for i in range(len(list1)):
#     output[list1[i]] = list2[i]
#
# print(output)

#
# output=dict(zip(list1,list2))
# print(output)

#
# output2={x:y for x,y in zip(list1,list2)}
# print(output2)

#
# output3=[x+y for x,y in (zip(list1,list2))]
# print(output3)


# Duplicate = [1, 2, 3, 4, 5, 6, 7, 8, 9, 2, 3, 4, 5, 9, 7]
# duplicates_list = []
# for i in Duplicate:
#     if Duplicate.count(i) > 1 and i not in duplicates_list:
#         duplicates_list.append(i)
#
# print(duplicates_list)



# list=["apple","Banana","cherry","watermelon","pineapple","kiwi","orange"]
# print(len(list))
list2=[]
# print(list[1])
# print(list[-1])
# print(list[2:5])
# print(list[0:4])
# print(list[1][::-1])
# for i in list:
#     if i == i:
#         list2.append(i[::-1])
# print(list2)
#

# for i  in list:
#     if i.startswith("B"):
#         list2.append(i)
# print(list2)

#
# fruits=str(input(" "))
#
# if fruits in list:
#     print("Yes it is present in thi list")
# else:
#     print("NO its not present in this list")
#


# list=["apple","Banana","cherry","watermelon","pineapple","kiwi","orange"]
# list2=[]

# for i in range(len(list)):
#     if list[i]=="Banana":
#         list[i]="cherry"
#     elif list[i]=="orange":
#          list[i]="kiwi"
# print(list)


# list.insert(1,"vishal")
# print(list)
#
# list.append("pooja")
# print(list)



# Extend list
# to append elements from one list to current  list we use extend() method:

# thislist = ["apple", "banana", "cherry"]
# tropical = ["mango", "pineapple", "papaya"]
# thislist.extend(tropical)
# print(thislist)


# thislist.remove("banana")
# print(thislist)


# l1=[1,2,3]
# l2=[4,5,6]
#
# l3=[x+y for x,y in zip(l1,l2)]
# print(l3)


# list1 = [1, 2, 3]
# list2 = [4, 5, 6]
# output=[x + y for x , y in zip(list1,list2)]
# print(output)


# fruits = ["apple", "banana", "cherry", "kiwi", "mango"]
# newlist=[]

# for i in fruits:
#     if "a" in i:
#         newlist.append(i)
# print(newlist)


# new_fruits=fruits.copy()
# print(new_fruits)

#
# thistuple = ("apple", "banana", "cherry")
# i=0
#
# while i< len(thistuple):
#     print(thistuple[i])
#     i+=1


#
# i=0
# while i<6:
#     print(i)
#     i+=1
#


# i=0
# while i<10:
#     print(i)
#     if i==5:
#         break
#     i+=1


# i=0
# while i<10:
#     i+=1
#     if i==6:
#         continue
#     print(i)


# i = 0
# while i < 6:
#   i += 1
#   if i == 3:
#     continue
#   print(i)


# fruits = ["apple", "banana", "cherry","apple", "banana", "cherry","apple", "banana", "cherry"]
# for i in fruits:
#     if i == "banana":
#         continue
#     print(fruits[i])


# fruits = ["apple", "banana", "cherry"]
# for x in fruits:
#   if x == "banana":
#     continue
#   print(x)



# def my_function(fname):
#   print(fname + "Refsnes")
#
# my_function("email")



# def functn(*kids):
#   print("this is youngest child" + kids[2])
#
# functn("vishal","pooja","vishal")



# def function(child1,child2,child3):
#   print("this is youngest child" + child3)
#
# function(child1="vishal",child2="pooja",child3="vaishnavi" )



# def function(food):
#   for x in food:
#     print(x)
# fruits = ["apple", "banana", "cherry"]
# function(fruits)




# def function(x):
#   return 5 * xGe
#
# print(function(5))



