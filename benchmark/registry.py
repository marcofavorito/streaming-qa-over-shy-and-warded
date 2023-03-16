from typing import TypeVar, Generic, Type, Mapping, Dict, Union

ItemId = TypeVar("ItemId")
Item = TypeVar("Item")


class ItemSpec(Generic[ItemId, Item]):
    """A specification for a particular instance of an object."""

    def __init__(
        self,
        item_id: ItemId,
        item_cls: Type[Item],
        **kwargs: Mapping,
    ) -> None:
        """
        Initialize an item specification.

        :param id_: the id associated to this specification
        :param item_cls: the item class
        :param kwargs: other custom keyword arguments.
        """
        self.item_id = item_id
        self.item_cls = item_cls
        self.kwargs = {} if kwargs is None else kwargs

    def make(self, **kwargs: Mapping) -> Item:
        """
        Instantiate an instance of the item object with appropriate arguments.

        :param kwargs: the key word arguments
        :return: an item
        """
        _kwargs = self.kwargs.copy()
        _kwargs.update(kwargs)
        item = self.item_cls(self.item_id, **_kwargs)
        return item


class ItemRegistry(Generic[ItemId, Item]):
    """Item registry."""

    item_id_cls: Type[ItemId]

    def __init__(self):
        """Initialize the registry."""
        self._specs: Dict[ItemId, ItemSpec[ItemId, Item]] = {}

    def register(
        self, item_id: Union[str, ItemId], item_cls: Type[Item], **kwargs: Mapping
    ):
        """Register a item."""
        item_id = self.item_id_cls(item_id)
        self._specs[item_id] = ItemSpec[ItemId, Item](item_id, item_cls, **kwargs)

    def make(self, item_id: Union[str, ItemId], **kwargs) -> Item:
        """
        Make the item.

        :param item_id: the item ID
        :param kwargs: the overrides for keyword arguments
        :return: the item instance
        """
        item_id = self.item_id_cls(item_id)
        if item_id not in self._specs:
            raise ValueError(f"item id '{item_id}' not configured")
        item_spec = self._specs[self.item_id_cls(item_id)]
        return item_spec.make(**kwargs)
