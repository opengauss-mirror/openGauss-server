/*
 * Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 *
 * openGauss is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 *          http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 *---------------------------------------------------------------------------------------
 *
 *  bayesnet_vector.h
 *
 * IDENTIFICATION
 *        src/include/db4ai/bayesnet_vector.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef DB4AI_BAYESNET_VECTOR_H
#define DB4AI_BAYESNET_VECTOR_H

#include <cstddef>

using namespace std;

template <typename T>
class Vector {
   public:
    typedef T *iterator;

   public:
    Vector() {
        this->p = (T *)palloc0(baseCapacity * sizeof(T));
        this->myCapacity = baseCapacity;
        this->mySize = 0;
    }

    void initialize_vector() {
        this->p = (T *)palloc0(baseCapacity * sizeof(T));
        this->myCapacity = baseCapacity;
        this->mySize = 0;
        return;
    }

    Vector(uint32_t mySize) {
        this->myCapacity = baseCapacity + mySize;
        this->mySize = mySize;
        this->p = (T *)palloc0(myCapacity * sizeof(T));
    }

    ~Vector() {
        if (p != nullptr) {
            pfree(p);
            p = nullptr;
        }
    }

    Vector(const Vector &v) {
        this->myCapacity = v.myCapacity;
        this->mySize = v.mySize;
        this->p = (T *)palloc0(this->myCapacity * sizeof(T));
        for (size_t i = 0; i < this->mySize; i++) {
            this->p[i] = v[i];
        }
    }

    Vector(Vector &&v) {
        this->myCapacity = v.myCapacity;
        this->mySize = v.mySize;
        this->p = v.p;
        v.p = nullptr;
    }

    void push_back(const T &data) {
        if (this->mySize == this->myCapacity) {
            T *new_p = (T *)palloc0((this->myCapacity + capacityExpansionIncrement) * sizeof(T));
            for (size_t i = 0; i < this->mySize; i++) {
                new_p[i] = p[i];
            }
            this->myCapacity += capacityExpansionIncrement;
            pfree(p);
            p = new_p;
        }

        this->p[this->mySize] = data;
        this->mySize++;
    }

    void pop_back() {
        if (this->mySize > 1) {
            this->mySize--;
        }
    }

    T &back() { return *(end() - 1); }

    void clear() {
        this->myCapacity = 0;
        this->mySize = 0;
        pfree(this->p);
        p = nullptr;
    }

    void insert(iterator pos, T data) {
        int32_t istPos = pos - this->begin();
        if (pos >= this->begin() && pos <= this->end()) {
            if (this->mySize == this->myCapacity) {
                T *new_p = (T *)palloc0((this->myCapacity + capacityExpansionIncrement) * sizeof(T));
                for (size_t i = 0; i < mySize; i++) {
                    new_p[i] = this->p[i];
                }
                this->myCapacity += capacityExpansionIncrement;
                pfree(this->p);
                this->p = new_p;
            }

            int32_t curPos = 0;
            for (curPos = mySize; curPos > istPos; curPos--) {
                this->p[curPos] = this->p[curPos - 1];
            }

            this->p[curPos] = data;
            this->mySize++;
        }
    }

    T &operator[](uint32_t index) const { return this->p[index]; }

    Vector operator=(const Vector &v) {
        this->myCapacity = v.myCapacity;
        this->mySize = v.mySize;
        this->p = (T *)palloc0(this->myCapacity * sizeof(T));
        for (size_t i = 0; i < this->mySize; i++) {
            this->p[i] = v[i];
        }
        return *this;
    }

    Vector operator=(Vector &&other) {
        if (this->p) {
            pfree(this->p);
        }
        this->mySize = other.mySize;
        this->myCapacity = other.myCapacity;
        this->p = other.p;
        other.p = nullptr;
        return *this;
    }

    size_t size() const { return this->mySize; }

    iterator end() { return this->p + this->size(); }

    iterator begin() { return this->p; }

   public:
    T *p{nullptr};
    uint32_t myCapacity{0};
    uint32_t mySize{0};
    static const uint32_t baseCapacity = 64U;
    static const uint32_t capacityExpansionIncrement = 256U;
};

#endif
