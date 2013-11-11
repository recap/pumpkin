grep -r --no-filename import *.py . | sed -e 's/^[ \t]*//' | grep -v ^# | grep ^import | sort -k2 | uniq | awk '{print "try:\n\t"$0"\nexcept ImportError as er:\n\tea = str(er).split(\" \")\n\tmod = ea[len(ea)-1].split(\".\")[0]\n\tprint mod\n\tpass\n"}' > ./imports.pyt
python imports.pyt | sort | uniq 
#rm imports.pyt

#echo ""
#echo "--------------------------------------"
#echo "Install with PIP:"
#echo "sudo pip install \`./pyld.sh\`"
#echo ""
#echo "--------------------------------------"
#echo "Install from apt on Ubuntu"
#echo "sudo apt-get install \`./pyld.sh\` | awk '{print \"python-\"$0}'"
#echo ""
#echo "--------------------------------------"

