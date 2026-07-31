# 扩展性<a name="ZH-CN_TOPIC_0289900647"></a>

GIN索引的接口实现了一个高层次的抽象，要求访问用户仅需要实现被访问数据类型的语义。GIN层自身可以处理并发操作、记录日志、搜索树结构的任务。

定义GIN索引的访问方式所要做的事情就是实现多个用户定义的方法，这些方法定义了键在树中的行为、键与键之间的关系、需要索引的item、能够使用索引的查询。简而言之，GIN索引将扩展性与普遍性、代码重用、清晰的接口结合在了一起。

实现GIN索引的操作符类有如下四个方法：

- int compare\(Datum a, Datum b\)

    比较两个key（不是索引的item）然后返回一个小于零、零或大于零的值，分别表示第一个key小于、等于或大于第二个key。NULL不会被传入这个函数。

- Datum \*extractValue\(Datum itemValue, int32 \*nkeys, bool \*\*nullFlags\)

    给定一个要被索引的item，返回一个对应key的数组。返回key的数目必须存储在\*nkeys中。如果任何key都可能为NULL，还要分配包含\*nkeys个布尔元素的数组，将地址存储到\*nullFlags，并且根据需要设置NULL值。 如果所有key都是非NULL，可以让\*nullFlags保持为NULL（他的初始值）。如果item不包含任何key，返回值可以为NULL。

- Datum \*extractQuery\(Datum query, int32 \*nkeys, StrategyNumber n, bool \*\*pmatch, Pointer \*\*extra\_data, bool \*\*nullFlags, int32 \*searchMode\)

    给定一个被查询的值，返回一个对应的key的数组。也就是说，query是可索引操作符右侧的值，而该操作符左侧是被索引的字段。 n是操作符类中操作符的策略号。通常，extractQuery需要参考n来决定query的数据类型以及抽取键值的方法。返回key的个数必须存放在\*nkeys中。如果任何key都可能为NULL，还要分配包含\*nkeys个布尔元素的数组，将地址存储到\*nullFlags，并且根据需要设置NULL值。 如果所有key都是非NULL的，可以让\*nullFlags保持为NULL（他的初始值）。如果query不包含任何key，返回值可以为NULL。

    searchMode是一个输出参数，他允许extractQuery指定一些关于如何执行搜索的细节。如果设置\*searchMode为GIN\_SEARCH\_MODE\_DEFAULT（这也是调用函数前此参数的初始化值），只有那些至少返回一个key的item才能被考虑作为候选匹配项。如果设置\*searchMode为GIN\_SEARCH\_MODE\_INCLUDE\_EMPTY，除了包含至少一个匹配key的item之外，根本不包含任何key的item也被考虑作为候选匹配项。（这个模式对于实现像“是否是子集”这样的操作是有用的）如果设置\*searchMode为GIN\_SEARCH\_MODE\_ALL，索引中所有非NULL的item都被考虑作为候选匹配项，不管他们是否匹配返回key中的任何一个。

    pmatch是一个允许支持部分匹配的输出参数。如果使用此参数，extractQuery必须分配有\*nkeys个布尔元素的数组，并把数组地址保存到\*pmatch。如果需要部分匹配相应的key，则数组的每个元素应该设置为TRUE；如果不需要匹配，则设置为FALSE。如果设置\*pmatch为NULL，则假设GIN不需要部分匹配。在函数调用前这个值被初始化为NULL，因此，对于不支持部分匹配的操作符类，可以忽略这个参数。

    extra\_data是一个允许extractQuery以consistent和comparePartial的方式传递额外数据的输出参数。如果使用他，extractQuery必须分配一个包含\*nkeys个Pointer元素的数组，并把数组地址保存到\*extra\_data，然后把他想附加的东西存储到各个独立的指针中。在函数调用前这个值初始化为NULL，因此，对于不需要附加数据的操作符类，可以忽略这个参数。如果设置了\*extra\_data，那么以consistent方式传递整个数组，使用comparePartial方式传递适当的元素。

- bool consistent\(bool check\[\], StrategyNumber n, Datum query, int32 nkeys, Pointer extra\_data\[\], bool \*recheck, Datum queryKeys\[\], bool nullFlags\[\]\)

    如果被索引项满足StrategyNumber为n的查询操作符则返回TRUE。这个函数并不直接访问被索引项的值，因为GIN并没有精确的把项目保存下来，但是需要知道从查询中提取的哪些键值出现在给定的被索引项中。 check数组的长度是nkeys，这个与query调用extractQuery函数返回的键值的数目相同。如果索引项包含了相应的查询键，check数组中对应的元素值就是TRUE。比如，如果\(check\[i\] == TRUE\)，那么意味着extractQuery的结果数组的第i个键出现在索引项中。考虑可能会用到consistent方式，原始的query也被作为参数传入进来。与此相同的还有extractQuery函数返回的queryKeys\[\]和nullFlags\[\]。 extra\_data是extractQuery函数返回的额外数据数组，如果没有的话就是NULL。

    当extractQuery在queryKeys\[\]中返回一个NULL的键值，如果被索引项包含NULL键值，相应的check\[\]中的元素是TRUE。也就是说，check\[\]的语义很像IS NOT DISTINCT FROM。如果需要知道是通常值匹配还是NULL匹配，consistent函数可以检查相应的nullFlags\[\]元素。

    成功执行后，如果堆元组需要针对查询运算符进行重新检查，\*recheck需要设置为TRUE，如果索引测试已经是精确的了，则设为FALSE。也就是说，FALSE的返回值确保堆元组不匹配这个查询；设置\*recheck为FALSE的TRUE的返回值确保堆元组匹配这个查询；设置\*recheck为TRUE的TRUE的返回值意味着堆元组可能匹配这个查询，因此需要通过直接对照原始索引项对查询运算符进行获取和重新检查。

GIN操作符类可以可选地提供第五个函数。

- int comparePartial\(Datum partial\_key, Datum key, StrategyNumber n, Pointer extra\_data\)

    比较一个部分匹配查询键和一个索引键。返回一个整型值，它的符号代表了不同的含义：小于0意味着索引键不匹配查询，但是索引扫描应该继续； 0意味着索引键匹配查询；大于0指示应该终止索引扫描，因为不可能再有更多的匹配。在需要确定何时结束扫描的语义的情况下，这里提供了生成部分一致查询的操作符的策略号n。同样的，extra\_data是extractQuery生成的额外数据数组中的相应元素，如果没有对应的元素，则为NULL。 NULL的键永远不会被传入这个函数。

为了支持"部分匹配"查询，一个操作符类必须提供comparePartial方法，并且当遇到部分匹配查询时，他的extractQuery方法必须设置pmatch参数。详细信息请参考[部分匹配算法](./implement.md#zh-cn_topic_0283137368_zh-cn_topic_0237122201_zh-cn_topic_0059778495_s9dc41ea95b9144c38d709b0b9a43fe9e)。

上面的各种Datum值的实际数据类型根据操作符类的不同而不同。传入到extractValue中的项目值总是操作符类的输入类型，所有的键值类型必须是这个类的STORAGE类型。传入到extractQuery和consistent的query参数的类型是由策略号识别的类成员操作符的右操作数的输入类型。他不需要和项目类型相同，只要可以从中抽取出正确类型的键值。
