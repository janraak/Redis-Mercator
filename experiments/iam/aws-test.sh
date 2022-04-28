

# sleep 60

echo "AKIAW2ZF3KPCX34Z2OME" >cred.txt
echo "$1" >cred.txt
echo "$2" >>cred.txt
echo "" >>cred.txt
echo "" >>cred.txt
echo "" >>cred.txt
aws configure <cred.txt >/dev/null
aws iam get-user
# # AWS_PROFILE=root aws iam create-user --user-name demo-001
# # AWS_PROFILE=root aws iam  add-user-to-group --group-name default-redis --user-name demo-100
# #  AWS_PROFILE=root aws iam create-access-key --user-name demo-100
# {
#     "AccessKey": {
#         "UserName": "demo-001",
#         "AccessKeyId": "AKIAW2ZF3KPCXZBMY3S3",
#         "Status": "Active",
#         "SecretAccessKey": "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",
#         "CreateDate": "2021-06-21T23:46:35Z"
#     }
# }
# # hello 3 auth AKIAW2ZF3KPCXZBMY3S3 'OV+bQZCRoqbB7olHn/xa6XMQOA6vTO7PBNPiOXWI'
# #  acl setuser AKIAW2ZF3KPCX34Z2OME ON >ze00L3N+vz3kBPqQXCraXD+GOUVSWKCLWIoNhKo2 +@all + allkeys
# # auth AKIAW2ZF3KPCX34Z2OME ze00L3N+vz3kBPqQXCraXD+GOUVSWKCLWIoNhKo2
# echo "AKIAW2ZF3KPCX34Z2OME" >cred.txt
# echo "ze00L3N+vz3kBPqQXCraXD+GOUVSWKCLWIoNhKo2" >>cred.txt
# echo "" >>cred.txt
# echo "" >>cred.txt
# cat cred.txt
# aws configure <cred.txt
# aws iam get-user

# # auth AKIAW2ZF3KPC7HEWWOVE 0ZQUYjPUvzUdzpc77eOlLTVUxPLqCrwVDunaNxXX
# echo "AKIAW2ZF3KPC7HEWWOVE" >cred.txt
# echo "0ZQUYjPUvzUdzpc77eOlLTVUxPLqCrwVDunaNxXX" >>cred.txt
# echo "" >>cred.txt
# echo "" >>cred.txt
# cat cred.txt
# aws configure <cred.txt
# aws iam get-user
