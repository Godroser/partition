#include<iostream>
#include<vector>
#include<queue>
#include<algorithm>

using namespace std;

int steal(int n, vector<int> nums){
	if (n == 1) return nums[0];
	if (n == 2) return max(nums[0], nums[1]);
	return max(steal(n-1, nums), steal(n-2, nums) + nums[n-1]);
}

int main(){
	int n;
	vector<int> nums;
	cin >> n;
	for (int i=0; i<n; i++){
		int tmp;
		cin >> tmp;
		nums.push_back(tmp);
	}


	cout << steal(n, nums) << endl;
	return 0;
}